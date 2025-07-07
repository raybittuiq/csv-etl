import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed

from advanced_infra.constants import FINAL_CSV
from advanced_infra.csv_manager import read_csv_in_chunks, process_and_write_chunk
from advanced_infra.db_loader import insert_into_database, logger


def run_pipeline(input_csv, chunk_size, db_path):

    temporary_files = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i, chunk in enumerate(read_csv_in_chunks(input_csv, chunk_size)):
            futures.append(executor.submit(process_and_write_chunk, chunk, i))

        for future in as_completed(futures):
            result = future.result()
            if result:
                temporary_files.append(result)

    # Combine all temp chunks into one CSV
    combined_df = pd.concat([pd.read_csv(f) for f in temporary_files])
    combined_df.to_csv(FINAL_CSV, index=False)
    logger.info(f"Combined processed CSV written to {FINAL_CSV}")

    # Load combined CSV into database
    for chunk in pd.read_csv(FINAL_CSV, chunksize=chunk_size):
        insert_into_database(chunk, db_path=db_path)

    logger.info("Data insertion complete.")