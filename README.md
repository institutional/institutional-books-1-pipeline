> 🚧 Work in progress 

# hlbooks-pipeline
A pipeline for analyzing, refining and publishing a dataset from Harvard Library's Google Books collection.

Commands are grouped as follows:
- **setup**: Pipeline setup and corpus I/O (for example: downloading and indexing a local copy of the collection).
- **analyze**: Analyze data already present in the collection (raw data, existing metadata). Results are stored in the database.

---

## Summary 
- [Getting started](#getting-started)
- [Available utilities](#available-utilities)
- [CLI: `setup`](#cli-setup)

---

## Getting started 

**Machine-level dependencies:**
- [Python 3.12+](https://python.org)
- [Python Poetry](https://python-poetry.org/)
- [Protobuf](https://github.com/protocolbuffers/protobuf) (`protobuf-compiler` on Debian/Ubuntu)
- [SQLite 3.32.0+](https://www.sqlite.org/)

```bash
# Clone project
git clone https://github.com/instdin/hlbooks-pipeline.git

# Install dependencies
bash install.sh

# Edit environment variables
nano .env # (or any text editor)

# Open python environment and pull source data / build the local database
poetry shell
python pipeline.py data build
```

[👆 Back to the summary](#summary)

---

## Available utilities

The following code excerpt presents some of the utilities this codebase makes available to work with the collection.

This codebase uses [Peewee as an ORM](https://docs.peewee-orm.com/en/latest/) to manage a [SQLite](https://www.sqlite.org/) database.

```python
import utils
from models import BookIO

# `BookIO` is a Peewee model for the "book_io" table.
# See Peewee's documentation for more info on how to work with models:
# https://docs.peewee-orm.com/en/latest/

# Retrieving an individual book by barcode
book = book.get(barcode="ABCDEF")
print(book.jsonl_data) # The full JSONL data is not stored at database level, but retrieved on the fly.
print(book.csv_data) # ... same goes for data coming from books.csv
print(book.tarball) # .tar.gz bytes of the underlying archive for the current book (raw data, cached).
print(book.images) # Raw scans, decompressed from .tar.gz 

# Iterating over the collection
for book in Book.select().iterator():
    print(book)

# Quick access to the Peewee db connector itself
db = utils.get_db()
```

All [models](/models/) cross-reference `BookIO` via a `book` foreign key.

[👆 Back to the summary](#summary)

---

## CLI: setup 

<details>
<summary><h3>setup build</h3></summary>

> ⚠️ This command must be run at least once.

Prepares the pipeline:
- Creates database tables
- Downloads source files from the output of GRIN-TO-S3 that was saved on S3/R2
- Indexes individual records from both JSONL and CSV files so BookIO can perform random access

```bash
python pipeline.py setup build
python pipeline.py setup build --update # Overwrite existing source files
python pipeline.py setup build --tables-only # Allows for only creating tables without populating them
```
</details>

<details>
<summary><h3>setup status</h3></summary>

Reports on the pipeline's status (database and cache size, etc ...)

```bash
python pipeline.py setup status
```

</details>

<details>
<summary><h3>setup clear</h3></summary>

Clears local data. Asks for confirmation before deleting each top-level folder/item.

```bash
python pipeline.py setup clear
```

</details>

[👆 Back to the summary](#summary)

---