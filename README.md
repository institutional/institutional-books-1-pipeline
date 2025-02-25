> 🚧 Work in progress 

# hlbooks-pipeline
A pipeline for analyzing, refining and publishing a dataset from Harvard Library's Google Books collection.

Commands are grouped as follows:
- **setup**: Pipeline setup and corpus I/O (for example: downloading and indexing a local copy of the collection).

---

## Summary 
- [Getting started](#getting-started)
- [Available utilities](#available-utilities)
- [CLI: `setup build`](#cli-setup-build)

---

## Getting started 

**Machine-level dependencies:**
- [Python 3.11+](https://python.org)
- [Python Poetry](https://python-poetry.org/)
- [Protobuf](https://github.com/protocolbuffers/protobuf) (`protobuf-compiler` on Debian/Ubuntu)

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

# Iterating over the collection
for book in Book.select().iterator():
    print(book)

# Quick access to the Peewee db connector itself
db = utils.get_db()
```

All [models](/models/) cross-reference `BookIO` via a `book` foreign key.

[👆 Back to the summary](#summary)

---
