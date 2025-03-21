import os
from pathlib import Path

import peewee

from const import OUTPUT_DATABASE_DIR_PATH, OUTPUT_DATABASE_FILENAME


def get_db() -> peewee.SqliteDatabase:
    """
    Creates and returns a connection to the local SQLite database.
    """
    os.makedirs(OUTPUT_DATABASE_DIR_PATH, exist_ok=True)
    db_filepath = Path(OUTPUT_DATABASE_DIR_PATH, OUTPUT_DATABASE_FILENAME)

    db = peewee.SqliteDatabase(
        str(db_filepath),
        pragmas={
            "journal_mode": "wal",
            "cache_size": -1 * 64000,
            "foreign_keys": 1,
            "ignore_check_constraints": 0,
            "busy_timeout": 5000,
            "temp_store": "memory",
        },
        timeout=20,
    )

    try:
        assert db.connect()
    except AssertionError:
        raise ConnectionError(f"Could not connect to {db_filepath}.")

    return db
