import sqlite3

from advanced_infra.constants import TABLE_NAME
from advanced_infra.logger_config import logger


def init_db(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                transaction_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount REAL NOT NULL,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL,
                processed_at TEXT NOT NULL
            );
        """)
    logger.info('Database initialized.')


def insert_into_database(chunk, db_path):
    with sqlite3.connect(db_path) as conn:
        try:
            conn.executemany(f"""
                            INSERT INTO {TABLE_NAME}
                            (transaction_id, user_id, amount, timestamp, status, processed_at)
                            VALUES (?, ?, ?, ?, ?, ?)""",
                             chunk.to_records(index=False))
            logger.info(f"Inserted {len(chunk)} records into table {TABLE_NAME}")
        except Exception as e:
            logger.error(f"Failed to insert {len(chunk)} records into {TABLE_NAME}: {e}")
