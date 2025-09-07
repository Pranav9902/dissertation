import pandas as pd
import numpy as np
import os

# --- Load merged dataset ---
df = pd.read_csv('final_merged_dataset_cleaned.csv', low_memory=False, parse_dates=['date', 'injured_since', 'injured_until'])

# --- Ensure date columns are correct ---
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
if 'injured_since' in df.columns:
    df['injured_since'] = pd.to_datetime(df['injured_since'], errors='coerce')
if 'injured_until' in df.columns:
    df['injured_until'] = pd.to_datetime(df['injured_until'], errors='coerce')

# --- Keep rows with valid match and injury info ---
df_filtered = df.copy()
df_filtered = df_filtered[df_filtered['date'].notna()]

# --- Extract EPL Season from date ---
def get_season(date):
    if pd.isna(date):
        return np.nan
    year = date.year
    if date.month >= 8:
        return f"{year}/{year+1}"
    else:
        return f"{year-1}/{year}"
df_filtered['season_epl'] = df_filtered['date'].apply(get_season)

# --- Base pre/post injury averages (if ratings available) ---
if 'minutes_played' in df_filtered.columns:
    df_filtered['minutes_played'] = pd.to_numeric(df_filtered['minutes_played'], errors='coerce').fillna(0)

# --- Minutes workload before injury ---
df_filtered = df_filtered.sort_values(['player_name_clean', 'date'])
df_filtered['minutes_workload_last5'] = (
    df_filtered.groupby('player_name_clean')['minutes_played']
    .transform(lambda x: x.rolling(window=5, min_periods=1).mean())
)

# --- Injury burden: total days missed per season ---
if 'injured_since' in df_filtered.columns and 'injured_until' in df_filtered.columns:
    df_filtered['injury_days'] = (df_filtered['injured_until'] - df_filtered['injured_since']).dt.days
    injury_burden = df_filtered.groupby(['player_name_clean', 'season_epl'])['injury_days'].sum().reset_index()
    injury_burden.rename(columns={'injury_days': 'injury_burden_days'}, inplace=True)
    df_filtered = df_filtered.merge(injury_burden, on=['player_name_clean', 'season_epl'], how='left')

# --- Form consistency: rolling std dev of minutes played ---
df_filtered['form_consistency_last5'] = (
    df_filtered.groupby('player_name_clean')['minutes_played']
    .transform(lambda x: x.rolling(window=5, min_periods=1).std())
)

# --- Recovery speed: # games until rating back to pre-injury avg ---
if all(col in df_filtered.columns for col in ['injured_since', 'injured_until']):
    recovery_games = []
    for player, group in df_filtered.groupby('player_name_clean'):
        group = group.sort_values('date')
        for _, row in group.iterrows():
            if pd.notna(row['injured_until']):
                post_injury = group[group['date'] > row['injured_until']]
                if not post_injury.empty and 'avg_rating_pre_injury' in group.columns:
                    pre_avg = group['avg_rating_pre_injury'].mean()
                    games_to_recover = (
                        post_injury[post_injury['avg_rating_post_injury'] >= pre_avg].head(1).shape[0]
                    )
                    recovery_games.append((player, row['season_epl'], games_to_recover))
    recovery_df = pd.DataFrame(recovery_games, columns=['player_name_clean', 'season_epl', 'recovery_games'])
    df_filtered = df_filtered.merge(recovery_df, on=['player_name_clean', 'season_epl'], how='left')

# --- Event period flag (pre/during/post) ---
def get_event_period(row):
    if pd.isna(row.get('injured_since')) or pd.isna(row.get('injured_until')) or pd.isna(row.get('date')):
        return np.nan
    if row['date'] < row['injured_since']:
        return 'pre'
    elif row['injured_since'] <= row['date'] <= row['injured_until']:
        return 'during'
    else:
        return 'post'
df_filtered['event_period'] = df_filtered.apply(get_event_period, axis=1)

# --- Save final dataset ---
df_filtered.to_csv('feature_engineered_dataset.csv', index=False)
print("✅ Feature engineering complete. Saved as 'feature_engineered_dataset.csv'")
