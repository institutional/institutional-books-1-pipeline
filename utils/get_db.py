import os
from pathlib import Path

import peewee

from const import DATABASE_DIR_PATH, DATABASE_FILENAME


def get_db() -> peewee.SqliteDatabase:
    """
    Creates and returns a connection to the local SQLite database.
    """
    os.makedirs(DATABASE_DIR_PATH, exist_ok=True)
    db_filepath = Path(DATABASE_DIR_PATH, DATABASE_FILENAME)

    db = peewee.SqliteDatabase(
        str(db_filepath),
        pragmas={
            "journal_mode": "wal",
            "cache_size": -1 * 64000,
            # "cache_size": -1 * 512000,
            "foreign_keys": 1,
            "ignore_check_constraints": 0,
            "busy_timeout": 10000,
            "temp_store": "memory",
        },
        timeout=30,
    )

    try:
        assert db.connect()
    except AssertionError:
        raise ConnectionError(f"Could not connect to {db_filepath}.")

    return db
