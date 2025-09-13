"""
Pre-Processing Script for EPL Datasets
Author: Pranav Prasanth

Description:
  Loads raw EPL match log, player summary, and injury datasets.
  Parses, cleans, and standardizes each dataset for downstream merging.
  Implements the workflow described in Section 3.5 of the dissertation.

Inputs:
  - epl_matchlogs_final_cleaned.csv
  - epl_player_summaries_clean.csv
  - epl_injuries_2015_2024_eplonly_cleaned.csv

Outputs:
  - intermediate/epl_matchlogs_primary.csv
  - intermediate/epl_player_summaries_cleaned.csv
  - intermediate/injury_data_raw.csv
"""

import pandas as pd
import os
import re

os.makedirs('intermediate', exist_ok=True)

# Load and save cleaned EPL matchlog
matchlog_df = pd.read_csv('epl_matchlogs_final_cleaned.csv', parse_dates=['date'])
matchlog_df.to_csv('intermediate/epl_matchlogs_primary.csv', index=False)

# Parse repeated summary columns in player summary dataset
def parse_summary_col(s):
    """
    Parse a summary column string of format 'Squad: 10, Starts: 9, Injured: 2' into a dict.
    Returns a dict mapping cleaned stat names to integer values.
    """
    if pd.isnull(s):
        return {}
    d = {}
    for part in s.split(","):
        if ":" in part:
            k, v = part.split(":", 1)
            k = k.strip().lower().replace(" ", "_")
            k = {
                "squad": "squad_appearances",
                "starting_eleven": "starts",
                "substituted_in": "subbed_in",
                "on_the_bench": "on_bench",
                "suspended": "suspended",
                "injured": "injured",
                "absence": "absence"
            }.get(k, k)
            d[k] = int(re.sub(r'[^\d]', '', v.strip()) or 0)
    return d

summaries_raw = pd.read_csv('epl_player_summaries_clean.csv')
summary_cols = [col for col in summaries_raw.columns if summaries_raw[col].astype(str).str.contains('Squad:', na=False).any()]
for col in summary_cols:
    expanded = summaries_raw[col].apply(parse_summary_col).apply(pd.Series).add_prefix(f"{col}_")
    summaries_raw = pd.concat([summaries_raw, expanded], axis=1)
summaries_cleaned = summaries_raw.drop(columns=summary_cols)
if 'player_name' in summaries_cleaned.columns:
    summaries_cleaned['player_name_clean'] = summaries_cleaned['player_name'].str.lower().str.strip()
summaries_cleaned.to_csv('intermediate/epl_player_summaries_cleaned.csv', index=False)

# Load and clean injury dataset
injury_path = 'epl_injuries_2015_2024_eplonly_cleaned.csv'
if os.path.exists(injury_path):
    injury_df = pd.read_csv(injury_path, dayfirst=True, low_memory=False, encoding='utf-8')
    if 'Name' in injury_df.columns:
        injury_df['player_name_clean'] = injury_df['Name'].str.lower().str.strip()
    if 'Date of Injury' in injury_df.columns:
        injury_df['date_of_injury'] = pd.to_datetime(injury_df['Date of Injury'], errors='coerce')
    if 'Date of return' in injury_df.columns:
        injury_df['date_of_return'] = pd.to_datetime(injury_df['Date of return'], errors='coerce')
    injury_df.to_csv('intermediate/injury_data_raw.csv', index=False)
else:
    print("No injury data found.")
    
print("Preprocessing complete: EPL matchlog, summary, and injury datasets loaded and cleaned.")
