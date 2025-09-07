import pandas as pd
import numpy as np
import json
import os
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve
)
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from imblearn.over_sampling import SMOTE
from sklearn.impute import SimpleImputer

# Optional: Import XGBoost if available
try:
    from xgboost import XGBClassifier
    xgb_available = True
except ImportError:
    xgb_available = False
    print("⚠️ XGBoost not installed. Skipping XGBoost model.")

# =========================
# Load dataset
# =========================
df = pd.read_csv("feature_engineered_dataset.csv", parse_dates=["date"])
print(f"✅ Dataset loaded: {df.shape}")

# =========================
# Drop duplicates
# =========================
df = df.drop_duplicates()
print(f"After dropping duplicates: {df.shape}")

# =========================
# Drop leakage columns
# =========================
leakage_cols = [
    "is_during_injury", "games_missed", "injured_summary",
    "injury_days", "injury_burden_days"
]
df = df.drop(columns=[c for c in leakage_cols if c in df.columns], errors="ignore")

# =========================
# Injury Risk Prediction
# =========================
if "injury" in df.columns:
    X = df.select_dtypes(include=[np.number]).drop(columns=["injury"], errors="ignore")
    y = df["injury"]

    print("Injury distribution:")
    print(y.value_counts(normalize=True))

    # Train/test split (stratified to handle imbalance)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Drop columns with all missing values in X_train
    X_train = X_train.dropna(axis=1, how='all')

    # Impute missing values in X_train
    imputer = SimpleImputer(strategy="median")
    X_train = pd.DataFrame(imputer.fit_transform(X_train), columns=X_train.columns, index=X_train.index)

    # For consistency, drop the same columns from X_test and impute
    X_test = X_test[X_train.columns]
    X_test = pd.DataFrame(imputer.transform(X_test), columns=X_train.columns, index=X_test.index)

    # Balance classes with SMOTE (synthetic oversampling)
    smote = SMOTE(random_state=42, sampling_strategy=0.2)
    X_train_bal, y_train_bal = smote.fit_resample(X_train, y_train)
    print("After SMOTE:", np.bincount(y_train_bal))

    # =========================
    # Model Definitions
    # =========================
    models = {
        "RandomForest": RandomForestClassifier(n_estimators=300, class_weight="balanced", random_state=42),
        "LogisticRegression": LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42)
    }
    if xgb_available:
        models["XGBoost"] = XGBClassifier(
            n_estimators=300,
            scale_pos_weight=(y_train_bal.value_counts()[0] / y_train_bal.value_counts()[1]),
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42
        )

    results_all = {}

    for name, model in models.items():
        print(f"\nTraining {name}...")
        model.fit(X_train_bal, y_train_bal)
        y_probs = model.predict_proba(X_test)[:, 1]

        # Tune threshold for best F1
        best_f1, best_thresh = 0, 0.5
        for thresh in np.linspace(0.1, 0.9, 20):
            preds = (y_probs >= thresh).astype(int)
            f1 = f1_score(y_test, preds)
            if f1 > best_f1:
                best_f1, best_thresh = f1, thresh

        y_pred = (y_probs >= best_thresh).astype(int)

        # Get feature importances/coefficients
        if hasattr(model, "feature_importances_"):
            importance = dict(
                sorted(
                    zip(X_train.columns, model.feature_importances_),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]
            )
        elif hasattr(model, "coef_"):
            importance = dict(
                sorted(
                    zip(X_train.columns, np.abs(model.coef_[0])),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]
            )
        else:
            importance = {}

        results = {
            "n_samples": len(df),
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, zero_division=0),
            "recall": recall_score(y_test, y_pred, zero_division=0),
            "f1": best_f1,
            "roc_auc": roc_auc_score(y_test, y_probs),
            "best_threshold": best_thresh,
            "top_features": importance
        }
        results_all[name] = results

        # Save ROC curve
        os.makedirs("outputs", exist_ok=True)
        fpr, tpr, _ = roc_curve(y_test, y_probs)
        auc = results["roc_auc"]

        plt.figure(figsize=(6, 6))
        plt.plot(fpr, tpr, label=f"{name} (AUC = {auc:.2f})")
        plt.plot([0, 1], [0, 1], "k--", label="Random")
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.title(f"ROC Curve: {name} Injury Prediction")
        plt.legend(loc="lower right")
        plt.tight_layout()
        plt.savefig(f"outputs/roc_curve_{name}.png")
        plt.close()
        print(f"✅ {name} ROC curve saved to outputs/roc_curve_{name}.png")

    # Save results
    def convert_numpy(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    with open("ml_results_comparison.json", "w") as f:
        json.dump(results_all, f, indent=2, default=convert_numpy)
    print("✅ All ML model results saved to ml_results_comparison.json")

else:
    print("❌ No injury column found in dataset.")