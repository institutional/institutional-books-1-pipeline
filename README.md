> 🚧 Work in progress 

# hlbooks-pipeline
The Institutional Data Initiative's pipeline for analyzing and refining the HLBooks collection source materials in order to publish it as a dataset.  

**Commands are grouped as follows:**
- **setup**: Pipeline setup and corpus I/O (for example: downloading and indexing a local copy of  the collection).
- **analyze**: Analysis of the data present in the collection. Results are stored in the database.
- **process**: Processing and/or augmentation of data from the collection.
- **export**: Export of samples and stats. 
- **publish**: Prepares the dataset for publication. 

---

## Summary 
- [Getting started](#getting-started)
- [Available utilities](#available-utilities)
- [CLI: `setup`](#cli-setup)
- [CLI: `analyze`](#cli-analyze)
- [CLI: `process`](#cli-process)
- [CLI: `export`](#cli-export)
- [CLI: `publish`](#cli-publish)

---

## Getting started 

**Machine-level dependencies:**
- [Python 3.12](https://python.org)
- [Python Poetry](https://python-poetry.org/) (recommended)
- [SQLite 3.32.0+](https://www.sqlite.org/)

```bash
# Clone project
git clone https://github.com/instdin/hlbooks-pipeline.git

# Install dependencies
# NOTE: Will attempt to install system-level dependencies on MacOS and Debian-based systems.
bash install.sh

# Edit environment variables
nano .env # (or any text editor)

# Open python environment and pull source data / build the local database
poetry shell # OR, for newer versions of poetry: eval $(poetry env activate)
python pipeline.py setup build
```

[👆 Back to the summary](#summary)

---

## Available utilities

The following code excerpt presents some of the utilities this codebase makes available to work with the collection. 

These are fairly specific to the way raw materials are currently organized on our storage backend, generated using `grin-to-s3`, our experimental tool for extracting a collection out of Google Books' backend. 

This codebase uses [Peewee as an ORM](https://docs.peewee-orm.com/en/latest/) to manage a [SQLite](https://www.sqlite.org/) database.

```python
import utils
from models import BookIO, BookRawData

# `BookIO` is a Peewee model for the "book_io" table.
# See Peewee's documentation for more info on how to work with models:
# https://docs.peewee-orm.com/en/latest/

# Retrieving an individual book by barcode
book = book.get(barcode="ABCDEF")

# Google-provided OCR text by page (list)
text: list[str] = book.text

# Metadata from xyz-books.csv (random access from disk)
csv_data: dict = book.csv_data

# Metadata and OCR text from xyz-0001.jsonl (random access fron disk)
jsonl_data: dict = book.jsonl_data

# Scans, OCR data, text exports and metadata and checksum extracted from barcode.tar.gz (pulled on the fly and cached)
raw_data: BookRawData = book.raw_data

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

Initializes the pipeline: 
- Creates the local database and its tables
- Downloads source files from the output of `grin-to-s3`, hosted on S3 or R2
- Indexes records within individual CSV and JSONL files so `BookIO` can perform fast random access on any barcode.

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