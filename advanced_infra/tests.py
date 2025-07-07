import pandas as pd
import sqlite3

from advanced_infra.csv_manager import transform_chunk
from advanced_infra.db_loader import init_db, TABLE_NAME, insert_into_database


# Sample input DataFrame for testing transformation
def test_transform_chunk():
    raw_data = pd.DataFrame([
        {"transaction_id": "tx1", "user_id": "u1", "amount": 100, "timestamp": "2025-07-06 12:00:00", "status": " Completed "},
        {"transaction_id": "tx2", "user_id": "u2", "amount": -50, "timestamp": "2025-07-06 12:01:00", "status": "pending"},
        {"transaction_id": "tx3", "user_id": "u3", "amount": "abc", "timestamp": "2025-07-06 12:02:00", "status": "pending"},
        {"transaction_id": "tx4", "user_id": "u4", "amount": 10, "timestamp": "2025-07-06 12:02:00",
         "status": "Cancelled"},
    ])

    transformed = transform_chunk(raw_data)

    # Only the first row should survive filtering
    assert transformed.shape[0] == 1
    assert transformed.iloc[0]['status'] == "completed"
    assert transformed.iloc[0]['amount'] == 100
    assert 'processed_at' in transformed.columns


def test_db_insertion():
    # Setup test database path
    test_db_path = "test_transactions.db"

    raw_data = pd.DataFrame([
        {"transaction_id": "tx3", "user_id": "u3", "amount": "abc", "timestamp": "2025-07-06 12:02:00",
         "status": "pending", "prccessed_at": "2025-07-06 12:02:00"},
        {"transaction_id": "tx4", "user_id": "u4", "amount": 10, "timestamp": "2025-07-06 12:02:00",
         "status": "pending", "prccessed_at": "2025-07-06 12:02:00"},
    ])

    # Initialize DB and insert
    init_db(db_path=test_db_path)
    insert_into_database(raw_data, db_path=test_db_path)
    with sqlite3.connect(test_db_path) as conn:
        cursor = conn.execute(f"SELECT * FROM {TABLE_NAME} order by transaction_id ASC")
        rows = cursor.fetchall()
        conn.execute(f"drop table {TABLE_NAME}")

    assert len(rows) == 2
    assert rows[0][0] == 'tx3'
    assert rows[1][0] == 'tx4'

