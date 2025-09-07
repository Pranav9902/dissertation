import pandas as pd
import os

matchlog_df = pd.read_csv('intermediate/epl_matchlogs_cleaned.csv', parse_dates=['date'])
summaries_df = pd.read_csv('intermediate/epl_player_summaries_cleaned_final.csv')
injury_path = 'intermediate/injury_data_cleaned.csv'

# Standardize player names
matchlog_df['player_name_clean'] = matchlog_df['player_name'].str.lower().str.strip()
summaries_df['player_name_clean'] = summaries_df['player_name_clean'].str.lower().str.strip()

# Merge matchlog with summary stats
merge_keys = ['player_name_clean', 'season', 'club_id']
merge_keys = [k for k in merge_keys if k in summaries_df.columns and k in matchlog_df.columns]
merged_df = pd.merge(matchlog_df, summaries_df, how='left', on=merge_keys)

# Merge in injury info if available
if os.path.exists(injury_path):
    injuries_df = pd.read_csv(injury_path, parse_dates=['injured_since', 'injured_until'])
    injuries_df['player_name_clean'] = injuries_df['player_name'].str.lower().str.strip()
    merged_df = pd.merge(merged_df, injuries_df, how='left', on='player_name_clean')
    # Create flag for if the player was injured during the match
    if 'injured_since' in merged_df.columns and 'injured_until' in merged_df.columns:
        merged_df['is_during_injury'] = merged_df.apply(
            lambda row: row['injured_since'] <= row['date'] <= row['injured_until']
            if pd.notnull(row['injured_since']) and pd.notnull(row['injured_until']) else False, axis=1
        )

merged_df.to_csv('final_merged_dataset.csv', index=False)
print("✅ Full merged dataset saved as 'final_merged_dataset.csv' (includes player summary stats and injury info).")