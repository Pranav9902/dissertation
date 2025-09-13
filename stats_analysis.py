"""
Statistical Analysis Script for EPL Injury and Performance Dataset
Author: Pranav Prasanth

Description:
  Performs descriptive and inferential statistical analyses on pre- and post-injury performance metrics,
  including paired t-tests and ANOVA. Outputs summary tables for reporting in the dissertation.
  This script implements the statistical methodology described in Section 3.8 of the dissertation.

Inputs:
  - feature_engineered_dataset.csv

Outputs:
  - outputs/episode_level_pre_post.csv       # Per-episode summary of pre/post metrics
  - outputs/stats_tests_summary.csv          # Results of paired t-tests
  - outputs/anova_by_position.csv            # ANOVA results by position
"""

import os
import numpy as np
import pandas as pd
from scipy.stats import ttest_rel, f_oneway

os.makedirs("outputs", exist_ok=True)

# --- Load feature-engineered dataset ---
DF_PATH = "feature_engineered_dataset.csv"
df = pd.read_csv(DF_PATH, low_memory=False, parse_dates=["date", "injured_since", "injured_until"])

# --- Ensure required columns exist for compatibility ---
for col in ["player_name", "player_name_clean"]:
    if col not in df.columns:
        df[col] = np.nan
if "player_name_clean" not in df.columns and "player_name" in df.columns:
    df["player_name_clean"] = df["player_name"].astype(str).str.lower().str.strip()
if "player_name" not in df.columns and "player_name_clean" in df.columns:
    df["player_name"] = df["player_name_clean"]

for c in ["minutes_played", "goals", "assists"]:
    if c not in df.columns:
        df[c] = 0
df["minutes_played"] = pd.to_numeric(df["minutes_played"], errors="coerce").fillna(0)
df["goals"] = pd.to_numeric(df["goals"], errors="coerce").fillna(0)
df["assists"] = pd.to_numeric(df["assists"], errors="coerce").fillna(0)

# --- Extract broad player position for stratified analysis ---
df["primary_position"] = df["position_type"].str.split(r"[,/]").str[0].str.strip()

def simplify_position(pos):
    """
    Simplify a position string to broad categories: GK, DF, MF, FW, or Hybrid.
    """
    if pd.isna(pos): return "Unknown"
    pos = pos.upper()
    if "GK" in pos: return "GK"
    if "DF" in pos: return "DF"
    if "MF" in pos: return "MF"
    if "FW" in pos: return "FW"
    return "Hybrid"

df["broad_position"] = df["primary_position"].apply(simplify_position)

# --- Unique match rows to avoid duplication from merges ---
matches = (
    df[["player_name_clean", "player_name", "date", "position_type", "minutes_played", "goals", "assists"]]
    .dropna(subset=["player_name_clean", "date"])
    .drop_duplicates(subset=["player_name_clean", "date"])
    .sort_values(["player_name_clean", "date"])
    .reset_index(drop=True)
)

# --- Calculate per-90-minute rates ---
def rate_per90(numer, mins):
    """
    Calculate a per-90-minutes rate, handling divide by zero.
    """
    mins = np.where(mins <= 0, np.nan, mins)
    return (numer / mins) * 90

matches["goals90"] = rate_per90(matches["goals"], matches["minutes_played"])
matches["assists90"] = rate_per90(matches["assists"], matches["minutes_played"])

# --- Identify injury episodes: each is a distinct range per player ---
episodes = (
    df.dropna(subset=["injured_since", "injured_until", "player_name_clean"])
      [["player_name_clean", "player_name", "injured_since", "injured_until", "position_type"]]
      .drop_duplicates()
      .sort_values(["player_name_clean", "injured_since"])
      .reset_index(drop=True)
)

WIN_PRE = 5   # Number of matches before injury to consider
WIN_POST = 5  # Number of matches after injury to consider

rows = []
for _, ep in episodes.iterrows():
    p = ep["player_name_clean"]
    pos = ep.get("position_type", np.nan)
    start = ep["injured_since"]
    end = ep["injured_until"]

    pm = matches[matches["player_name_clean"] == p].copy()
    if pm.empty or pd.isna(start) or pd.isna(end):
        continue

    # PRE: last WIN_PRE matches strictly before injury start
    pre = pm[pm["date"] < start].tail(WIN_PRE)
    # POST: first WIN_POST matches strictly after injury return
    post = pm[pm["date"] > end].head(WIN_POST)

    if pre.empty or post.empty:
        continue

    def agg(block):
        return pd.Series({
            "n_matches": len(block),
            "minutes_mean": block["minutes_played"].mean(),
            "goals90_mean": block["goals90"].mean(),
            "assists90_mean": block["assists90"].mean()
        })

    pre_agg = agg(pre)
    post_agg = agg(post)

    rows.append({
        "player_name_clean": p,
        "player_name": ep.get("player_name", p),
        "position_type": pos,
        "primary_position": str(pos).split(",")[0].split("/")[0].strip() if pd.notna(pos) else np.nan,
        "injured_since": start,
        "injured_until": end,
        "pre_n_matches": pre_agg["n_matches"],
        "pre_minutes_mean": pre_agg["minutes_mean"],
        "pre_goals90_mean": pre_agg["goals90_mean"],
        "pre_assists90_mean": pre_agg["assists90_mean"],
        "post_n_matches": post_agg["n_matches"],
        "post_minutes_mean": post_agg["minutes_mean"],
        "post_goals90_mean": post_agg["goals90_mean"],
        "post_assists90_mean": post_agg["assists90_mean"],
        "delta_minutes_mean": post_agg["minutes_mean"] - pre_agg["minutes_mean"],
        "delta_goals90_mean": post_agg["goals90_mean"] - pre_agg["goals90_mean"],
        "delta_assists90_mean": post_agg["assists90_mean"] - pre_agg["assists90_mean"],
    })

episode_level = pd.DataFrame(rows)
episode_level.to_csv("outputs/episode_level_pre_post.csv", index=False)

# --- Paired t-tests on pre/post differences (only for players with both) ---
test_results = []

def paired_ttest(pre_col, post_col, label):
    """
    Perform a paired t-test for pre- and post-injury metrics.
    """
    sub = episode_level[[pre_col, post_col]].dropna()
    if len(sub) >= 2:
        stat, p = ttest_rel(sub[post_col], sub[pre_col], nan_policy="omit")
        return {"metric": label, "n_pairs": int(len(sub)), "t_stat": float(stat), "p_value": float(p)}
    return {"metric": label, "n_pairs": int(len(sub)), "t_stat": np.nan, "p_value": np.nan}

test_results.append(paired_ttest("pre_minutes_mean", "post_minutes_mean", "Minutes (mean)"))
test_results.append(paired_ttest("pre_goals90_mean", "post_goals90_mean", "Goals/90 (mean)"))
test_results.append(paired_ttest("pre_assists90_mean", "post_assists90_mean", "Assists/90 (mean)"))

pd.DataFrame(test_results).to_csv("outputs/stats_tests_summary.csv", index=False)

# --- ANOVA on deltas by player position ---
anova_rows = []
if "primary_position" in episode_level.columns and episode_level["primary_position"].notna().any():
    tmp = episode_level.dropna(subset=["delta_goals90_mean", "primary_position"])
    groups = [g["delta_goals90_mean"].values for _, g in tmp.groupby("primary_position")]
    groups = [g[~np.isnan(g)] for g in groups if len(g[~np.isnan(g)]) > 1]
    if len(groups) >= 2:
        fstat, p = f_oneway(*groups)
        anova_rows.append({"metric": "delta_goals90_mean_by_primary_position", "k_groups": len(groups), "f_stat": float(fstat), "p_value": float(p)})

# (Optional) ANOVA by broad_position
if "broad_position" in episode_level.columns and episode_level["broad_position"].notna().any():
    tmp = episode_level.dropna(subset=["delta_goals90_mean", "broad_position"])
    groups = [g["delta_goals90_mean"].values for _, g in tmp.groupby("broad_position")]
    groups = [g[~np.isnan(g)] for g in groups if len(g[~np.isnan(g)]) > 1]
    if len(groups) >= 2:
        fstat, p = f_oneway(*groups)
        anova_rows.append({"metric": "delta_goals90_mean_by_broad_position", "k_groups": len(groups), "f_stat": float(fstat), "p_value": float(p)})

pd.DataFrame(anova_rows).to_csv("outputs/anova_by_position.csv", index=False)

print("✅ Saved:\n - outputs/episode_level_pre_post.csv\n - outputs/stats_tests_summary.csv\n - outputs/anova_by_position.csv")
