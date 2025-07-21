import pandas as pd

# Load injuries dataset
injuries_df = pd.read_csv('archive (2)/dataset.csv', low_memory=False)
print("Injuries dataset columns:", injuries_df.columns.tolist())

# Find the correct date column name from the printed list
# For example, if it's 'injury_date', use that:
date_col = 'injury_date'  # <-- change this to your actual date column name

injuries_df['p_id2_clean'] = injuries_df['p_id2'].str.lower().str.strip()
injuries_df['date_clean'] = pd.to_datetime(injuries_df[date_col]).dt.date

chunk_size = 100_000
first = True

for chunk in pd.read_csv('cleaned_statsbomb_events_with_dates.csv', low_memory=False, chunksize=chunk_size):
    chunk['player_name_clean'] = chunk['player.name'].str.lower().str.strip()
    chunk['match_date_clean'] = pd.to_datetime(chunk['match_date']).dt.date

    merged_chunk = pd.merge(
        chunk,
        injuries_df,
        how='left',
        left_on=['player_name_clean', 'match_date_clean'],
        right_on=['p_id2_clean', 'date_clean']
    )
    merged_chunk.drop(columns=['player_name_clean', 'p_id2_clean', 'match_date_clean', 'date_clean'], inplace=True)
    merged_chunk.to_csv('final_merged_dataset_with_dates.csv', mode='a', index=False, header=first)
    first = False

print("✅ Final merged dataset with dates saved as 'final_merged_dataset_with_dates.csv'")

df = pd.read_csv('final_merged_dataset_with_dates.csv', nrows=100000)
df.to_csv('sample_final_merged_dataset_with_dates.csv', index=False)
