from advanced_infra.constants import CHUNK_SIZE, DB_PATH
from advanced_infra.db_loader import init_db
from advanced_infra.pipeline import run_pipeline


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Large CSV ETL to SQLite")
    parser.add_argument("--input_csv", required=True, help="Path to input CSV file")
    parser.add_argument("--chunk_size", type=int, default=CHUNK_SIZE, help="Chunk size for processing")
    parser.add_argument("--db_path", default=DB_PATH, help="SQLite DB path")
    args = parser.parse_args()
    global chunk_size, db_path

    chunk_size = args.chunk_size
    db_path = args.db_path

    init_db(db_path)
    run_pipeline(args.input_csv, chunk_size, db_path)




