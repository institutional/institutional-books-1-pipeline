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
- [CLI: Common options](#cli-common-options)
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
python pipeline.py setup build # Must be run at least once!
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

## CLI: Common options

All of the CLI commands listed in this README have a `--help` flag that lists its options.

Here are common options:

| Option name | Description |
| --- | --- |
| `--overwrite` | Delete existing entries/files if they already exist |
| `--offset` and `--limit` | Allows for running an operation on a subset of `BookIO` entries. Entries are ordered by barcode. |
| `--max-workers` | For commands that spin up sub processes, allows for determining how many workers should be created. Generally defaults to the number of available CPU threads. |
| `--db-write-batch-size` | Allows for determining how many entries should be processed before writing to the database. Matters in a very limited number of contexts. |


[👆 Back to the summary](#summary)

---

## CLI: setup 

> ⚠️ `setup build` must be run at least once.

<details>
<summary><h3>setup build</h3></summary>

Initializes the pipeline: 
- Creates the local database and its tables
- Downloads source files from the output of `grin-to-s3`, hosted on S3 or R2
- Indexes records within individual CSV and JSONL files so `BookIO` can perform fast random access on any barcode.

```bash
python pipeline.py setup build
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

## CLI: setup 

<details>
<summary><h3>analyze extract-genre-classification-from-metadata</h3></summary>

Parses and stores genre or form classification data available in the collection's metadata for each book.

Notes:
- Extracted from `gxml Index Term-Genre/Form` (via `book.csv_data`)
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-genre-classification-from-metadata
```

</details>

<details>
<summary><h3>analyze extract-hathitrust-rights-determination</h3></summary>

Attempts to match Harvard Library's Google Books records with [Hathitrust's rights determination records](https://www.hathitrust.org/member-libraries/resources-for-librarians/data-resources/bibliographic-api/).
Stores the resulting matches in the database.

Notes:
- `--max-workers` defaults to 4.
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-hathitrust-rights-determination
```

</details>

<details>
<summary><h3>analyze extract-main-language-from-metadata</h3></summary>

Parses and stores book-level language classification data available in the collection's metadata for each book.

Notes:
- Extracted from `gxml Language` (via `book.csv_data`)
- Original data is in ISO 639-2B format. This command stores it both in this format as well as ISO 639-3.
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-main-language-from-metadata
```

</details>


[👆 Back to the summary](#summary)

---