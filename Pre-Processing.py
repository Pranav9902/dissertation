import pandas as pd
import json
import os
import glob
from tqdm import tqdm

folder_path = 'data/events'
all_files = glob.glob(os.path.join(folder_path, '*.json'))

batch_size = 100
temp_files = []
columns_order = None  # To store the columns order from the first batch

for i in range(0, len(all_files), batch_size):
    batch_files = all_files[i:i+batch_size]
    all_data = []
    for file in tqdm(batch_files, desc=f"Batch {i//batch_size+1}"):
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            df = pd.json_normalize(data)
            all_data.append(df)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        cleaned_df = combined_df.dropna(subset=['type.name'])
        if columns_order is None:
            columns_order = cleaned_df.columns.tolist()
        else:
            cleaned_df = cleaned_df.reindex(columns=columns_order)
        temp_csv = f'temp_batch_{i//batch_size+1}.csv'
        cleaned_df.to_csv(temp_csv, index=False)
        temp_files.append(temp_csv)

# Combine all temporary CSVs into the final output
output_file = 'all_cleaned_statsbomb_events.csv'
first = True
for temp_csv in temp_files:
    chunk = pd.read_csv(temp_csv, low_memory=False)
    chunk.to_csv(output_file, mode='a', index=False, header=first)
    first = False
    os.remove(temp_csv)  # Remove temp file after appending

print("✅ All event files processed in batches and saved to 'all_cleaned_statsbomb_events.csv'")

# Match information extraction
matches_folder = 'data/matches'
match_files = glob.glob(os.path.join(matches_folder, '**', '*.json'), recursive=True)

match_info = []
for file in match_files:
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        # If the file contains a list of matches
        if isinstance(data, list):
            for match in data:
                match_info.append({
                    'match_id': match['match_id'],
                    'match_date': match['match_date']
                })
        # If the file contains a single match dict
        elif isinstance(data, dict):
            match_info.append({
                'match_id': data['match_id'],
                'match_date': data['match_date']
            })

match_dates_df = pd.DataFrame(match_info)

# Merge on match_id
chunk_size = 100_000  # or smaller if needed
first = True

for chunk in pd.read_csv('all_cleaned_statsbomb_events.csv', low_memory=False, chunksize=chunk_size):
    # Rename 'id' to 'match_id' if necessary
    if 'id' in chunk.columns and 'match_id' not in chunk.columns:
        chunk.rename(columns={'id': 'match_id'}, inplace=True)
    # Ensure both are strings for merging
    chunk['match_id'] = chunk['match_id'].astype(str)
    match_dates_df['match_id'] = match_dates_df['match_id'].astype(str)
    merged_chunk = chunk.merge(match_dates_df, on='match_id', how='left')
    merged_chunk.to_csv('events_with_dates.csv', mode='a', index=False, header=first)
    first = False

print("✅ Events with match dates saved as 'events_with_dates.csv'")
