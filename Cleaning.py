import pandas as pd
import numpy as np

chunk_size = 100_000  # Adjust as needed
chunks = []
for chunk in pd.read_csv('events_with_dates.csv', low_memory=False, chunksize=chunk_size):
    # Apply your cleaning steps to each chunk
    chunk = chunk.dropna(subset=['type.name'])
    chunk.drop_duplicates(inplace=True)
    required_cols = [col for col in ['match_id', 'player.id'] if col in chunk.columns]
    if required_cols:
        chunk = chunk.dropna(subset=required_cols)
    missing_threshold = 0.3
    chunk = chunk.loc[:, chunk.isnull().mean() < missing_threshold]
    numeric_cols = chunk.select_dtypes(include=['float64', 'int64']).columns
    chunk[numeric_cols] = chunk[numeric_cols].fillna(chunk[numeric_cols].median())
    categorical_cols = chunk.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        chunk[col] = chunk[col].fillna(chunk[col].mode()[0] if not chunk[col].mode().empty else "Unknown")
    # Optional: remove outliers using Z-score
    from scipy.stats import zscore
    if not chunk[numeric_cols].empty:
        z_scores = np.abs(chunk[numeric_cols].apply(zscore))
        chunk = chunk[(z_scores < 3).all(axis=1)]
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)
df.to_csv('cleaned_statsbomb_events_with_dates_final.csv', index=False)
print("✅ Data cleaned and saved as 'cleaned_statsbomb_events_with_dates_final.csv'")
