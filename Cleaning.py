import pandas as pd
import numpy as np
import os

# --- Clean EPL Matchlog ---
df = pd.read_csv('intermediate/epl_matchlogs_primary.csv', parse_dates=['date'])
df = df.drop_duplicates()
df = df.dropna(subset=['player_name', 'matchday'])

numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
df[numeric_cols] = df[numeric_cols].fillna(0)
for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown')
df.to_csv('intermediate/epl_matchlogs_cleaned.csv', index=False)

# --- Clean EPL Player Summary Dataset ---
summaries = pd.read_csv('intermediate/epl_player_summaries_cleaned.csv')
summaries = summaries.drop_duplicates()
for col in summaries.select_dtypes(include='object').columns:
    summaries[col] = summaries[col].fillna(summaries[col].mode()[0] if not summaries[col].mode().empty else 'Unknown')
numeric_cols = summaries.select_dtypes(include=['float64', 'int64']).columns
summaries[numeric_cols] = summaries[numeric_cols].fillna(0)
summaries.to_csv('intermediate/epl_player_summaries_cleaned_final.csv', index=False)

# --- Clean Injury Data ---
injury_path = 'intermediate/injury_data_raw.csv'
if os.path.exists(injury_path):
    injuries = pd.read_csv(injury_path, dayfirst=True, low_memory=False)
    if 'date_of_injury' in injuries.columns:
        injuries['date_of_injury'] = pd.to_datetime(injuries['date_of_injury'], errors='coerce')
    if 'date_of_return' in injuries.columns:
        injuries['date_of_return'] = pd.to_datetime(injuries['date_of_return'], errors='coerce')
    injuries = injuries.drop_duplicates()
    for col in injuries.select_dtypes(include='object').columns:
        injuries[col] = injuries[col].fillna(injuries[col].mode()[0] if not injuries[col].mode().empty else 'Unknown')
    numeric_cols = injuries.select_dtypes(include=['float64', 'int64']).columns
    injuries[numeric_cols] = injuries[numeric_cols].fillna(0)
    injuries.to_csv('intermediate/injury_data_cleaned.csv', index=False)
else:
    print("No injury data to clean.")

print("✅ Cleaning complete: EPL matchlog, player summary, and injury data cleaned.")