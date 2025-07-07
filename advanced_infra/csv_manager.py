import os
import pandas as pd

from advanced_infra.logger_config import logger
from advanced_infra.constants import TEMP_DIR
from datetime import datetime, timezone


def read_csv_in_chunks(file_path, chunk_size):
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            yield chunk
    except Exception as e:
        logger.error(f"Failed to read CSV in chunks: {e}")
        raise


def write_temp_chunk(chunk, chunk_id):
    try:
        os.makedirs(TEMP_DIR, exist_ok=True)
        temp_file = os.path.join(TEMP_DIR, f"chunk_{chunk_id}.csv")
        chunk.to_csv(temp_file, index=False)
        return temp_file
    except Exception as e:
        logger.error(f"Failed to write chunk {chunk_id}: {e}")
        return None


def transform_chunk(chunk):
    try:
        chunk['status'] = chunk['status'].str.lower().str.strip()
        chunk['amount'] = pd.to_numeric(chunk['amount'], errors='coerce')
        chunk = chunk[(chunk['amount'] >= 0) & (chunk['status'] != 'cancelled')]
        processed_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        chunk['processed_at'] = processed_time
        chunk.dropna(subset=['transaction_id', 'user_id', 'amount', 'timestamp', 'status'], inplace=True)
        return chunk
    except Exception as e:
        logger.error(f"Failed to transform chunk: {e}")
        return pd.DataFrame()  # Return empty chunk on error

def process_and_write_chunk(chunk, chunk_id):
    transformed = transform_chunk(chunk)
    if not transformed.empty:
        return write_temp_chunk(transformed, chunk_id)
    return None