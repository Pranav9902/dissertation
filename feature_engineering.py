"""
Feature Engineering Script for EPL Dataset
Author: Pranav Prasanth

Description:
  Adds engineered features for machine learning modeling: rolling averages, injury burden, consistency, recovery, and event period.
  Implements the feature engineering steps described in Section 3.7 of the dissertation.

Input:
  - final_merged_dataset_cleaned.csv

Output:
  - feature_engineered_dataset.csv
"""

import pandas as pd
import numpy as np
import os

df = pd.read_csv('final_merged_dataset_cleaned.csv', low_memory=False, parse_dates=['date', 'injured_since', 'injured_until'])

# Ensure date columns are datetime
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
if 'injured_since' in df.columns:
    df['injured_since'] = pd.to_datetime(df['injured_since'], errors='coerce')
if 'injured_until' in df.columns:
    df['injured_until'] = pd.to_datetime(df['injured_until'], errors='coerce')

df_filtered = df[df['date'].notna()].copy()

# Extract season from match date
def get_season(date):
    if pd.isna(date):
        return np.nan
    year = date.year
    if date.month >= 8:
        return f"{year}/{year+1}"
    else:
        return f"{year-1}/{year}"
df_filtered['season_epl'] = df_filtered['date'].apply(get_season)

# Calculate rolling averages for minutes played (captures acute workload)
df_filtered = df_filtered.sort_values(['player_name_clean', 'date'])
df_filtered['minutes_workload_last5'] = (
    df_filtered.groupby('player_name_clean')['minutes_played']
    .transform(lambda x: x.rolling(window=5, min_periods=1).mean())
)

# Calculate total days missed per season (injury burden)
if 'injured_since' in df_filtered.columns and 'injured_until' in df_filtered.columns:
    df_filtered['injury_days'] = (df_filtered['injured_until'] - df_filtered['injured_since']).dt.days
    injury_burden = df_filtered.groupby(['player_name_clean', 'season_epl'])['injury_days'].sum().reset_index()
    injury_burden.rename(columns={'injury_days': 'injury_burden_days'}, inplace=True)
    df_filtered = df_filtered.merge(injury_burden, on=['player_name_clean', 'season_epl'], how='left')

# Rolling standard deviation of minutes played (form consistency indicator)
df_filtered['form_consistency_last5'] = (
    df_filtered.groupby('player_name_clean')['minutes_played']
    .transform(lambda x: x.rolling(window=5, min_periods=1).std())
)

# Assign event period: pre, during, post injury
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

df_filtered.to_csv('feature_engineered_dataset.csv', index=False)
print("Feature engineering complete. Saved as 'feature_engineered_dataset.csv'")
