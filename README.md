# 📚 Institutional Books Pipeline
The Institutional Data Initiative's pipeline for analyzing, refining, and publishing the Institutional Books 1.0 collection.

- 🤗 [Institutional Books on HuggingFace](https://huggingface.co/collections/instdin/institutional-books-68366258bfb38364238477cf)
- 📄 [Technical report](https://arxiv.org/abs/2506.08300)
- 🌐 [Website](https://institutionaldatainitiative.org/institutional-books)

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
git clone https://github.com/instdin/institutional-books-1-pipeline.git

# Install dependencies
# NOTE: Will attempt to install system-level dependencies on MacOS and Debian-based systems.
bash install.sh

# Edit environment variables
nano .env # (or any text editor)

# Open python environment and pull source data / build the local database
poetry shell # OR, for newer versions of poetry: eval $(poetry env activate)
python pipeline.py setup build # Must be run at least once!
```

**Commands are grouped as follows:**
- **setup**: Pipeline setup and corpus I/O (for example: downloading and indexing a local copy of  the collection).
- **analyze**: Analysis of the data present in the collection. Results are stored in the database.
- **process**: Processing and/or augmentation of data from the collection.
- **export**: Export of samples and stats. 
- **publish**: Prepares the dataset for publication. 

[👆 Back to the summary](#summary)

---

## Available utilities

The following code excerpt presents some of the utilities this codebase makes available to work with the collection. 

These are fairly specific to the way raw materials are currently organized on our storage backend, generated using our experimental tool for extracting a collection out of Google Books' backend. 

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
text: list[str] = book.text_by_page

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
- Creates the local database and its tables.
- Downloads source files from the output of `grin-to-s3`, hosted on S3 or R2.
- Indexes records within individual CSV and JSONL files so `BookIO` can perform fast random access on any barcode.

```bash
python pipeline.py setup build
python pipeline.py setup build --tables-only # Allows for only creating tables without populating them
```
</details>

<details>
<summary><h3>setup status</h3></summary>

Reports on the pipeline's status (database and cache size, etc ...).

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

## CLI: analyze 

<details>
<summary><h3>analyze extract-genre-classification-from-metadata</h3></summary>

Collects genre/form classification data for each book from the collection's metadata.

Notes:
- Extracted from `gxml Index Term-Genre/Form` (via `book.csv_data`).
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-genre-classification-from-metadata
```

</details>

<details>
<summary><h3>analyze extract-hathitrust-rights-determination</h3></summary>

Collects rights determination data from the [Hathitrust API](https://www.hathitrust.org/member-libraries/resources-for-librarians/data-resources/bibliographic-api/) for this collection.

Notes:
- `--max-workers` defaults to 4.
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-hathitrust-rights-determination
```

</details>

<details>
<summary><h3>analyze extract-main-language-from-metadata</h3></summary>

Collects book-level language data for each book from the collection's metadata.

Notes:
- Extracted from `gxml Language` (via `book.csv_data`)
- Original data is in ISO 639-2B format. This command stores it both in this format as well as ISO 639-3.
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-main-language-from-metadata
```

</details>

<details>
<summary><h3>analyze extract-ocr-quality-from-metadata</h3></summary>

Collects Google-provided OCR quality metrics for each book, as expressed in the collection's metadata.

Notes:
- Extracted from `OCR Analysis Score` (via `book.csv_data`).
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-ocr-quality-from-metadata
```

</details>

<details>
<summary><h3>analyze extract-page-count</h3></summary>

Extracts the page count of each book, both:
- as expressed in the collection's metadata (`Page Count` via `book.csv_data`)
- from the total of available pages in the OCR'd text

Notes:
- Skips entries that were already analyzed, unless instructed otherwise

```bash
python pipeline.py analyze extract-page-count
```

</details>

<details>
<summary><h3>analyze extract-topic-classification-from-metadata</h3></summary>

Collects topic/subject classification data for each book from the collection's metadata.

Notes:
- Extracted from `gxml Subject Added Entry-Topical Term` (via `book.csv_data`).
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-topic-classification-from-metadata
```

</details>

<details>
<summary><h3>analyze extract-topic-classification-training-dataset</h3></summary>

Collects topic classification items that can be used to train a text classification model.
Said text classification model's goal is to assign a top-level category from the [Library of Congress' Classification Outline](https://www.loc.gov/catdir/cpso/lcco/) to a given book based on its metadata.

Isolates entries where:
- `TopicClassification.from_metadata` only contains 1 term (no comma).
- Said term can be matched with one of the top-level items from the Library Of Congress Classification Outline (see `LOC_CO_TO_GXML_TOPICS`).

Notes:
- Replaces existing training set if already present.
- Training dataset is split between "train" (most entries), "test" (validation, 5000 entries), "benchmark" (1000 entries).
- See `export topic-classification-training-set` to export the results of this command.

```bash
python pipeline.py analyze extract-topic-classification-training-dataset
```

</details>

<details>
<summary><h3>analyze extract-year-of-publication-from-metadata</h3></summary>

Collects, for each entry, the likely year of publication based on existing metadata.
This is meant to be used for statistical analysis purposes only.

Notes:
- Extracted from either `gxml Date1 ` or `gxml Date 2` (via `book.csv_data`)
- Entries with where `gxml Date Type` is either `Continuing resource` or `No attempt to code` will be skipped.
- Incomplete years will be ignored (e.g: `19uu`, `1uuu`, `9999` ...)
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze extract-year-of-publication-from-metadata
```

</details>

<details>
<summary><h3>analyze run-language-detection</h3></summary>

Runs text-level language detection on the OCR'd text of each book, split into chunks.

For each book:
- Collects the distribution and proportion of all identified languages in `language_detection`.
- Keeps track of token counts identified per language, at book level. (`o200k_base` tokens).
- Keeps track of the "main" detected language in `main_language` (for comparison with metadata info).

Notes:
- Uses `pyfranc`.
- By default, texts are split and analyzed in blocks of up to 768 characters.
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze run-language-detection
```

</details>

<details>
<summary><h3>analyze run-ocr-quality-detection</h3></summary>

Runs [pleais/OCROScope](https://github.com/Pleias/OCRoscope) on the OCR'd text of each book in order to collect a secondary OCR quality metric.

Notes:
- Skips entries that were already analyzed, unless instructed otherwise

```bash
python pipeline.py analyze run-ocr-quality-detection
```

</details>

<details>
<summary><h3>analyze run-simhash</h3></summary>

Generate a simhash for every OCR'd text in the collection in order to coarsely identify collection-level near duplicates.

Notes:
- Skips entries that were already analyzed, unless instructed otherwise.

```bash
python pipeline.py analyze run-simhash
```

</details>

<details>
<summary><h3>analyze run-text-analysis</h3></summary>

Runs simple text analysis methods on the OCR'd text of each entry in the collection.

Collects metrics such as:
- character/word/bigram/trigram/sentence counts.
- token-type ratios.
- tokenizability (how "well" a given text tokenizes using `o200k_base`).

Notes:
- Skips entries that were already analyzed, unless instructed otherwise

```bash
python pipeline.py analyze run-text-analysis
```

</details>


<details>
<summary><h3>analyze run-token-count</h3></summary>

Tokenizes the OCR'd text of each entry and saves the resulting token counts in the database.
Uses the tokenizer of the target LLM specified via `--target-llm`.

Notes:
- `--target-llm` can identify both OpenAI and HuggingFace-hosted models. Prefix with `openai/` for OpenAI models.
- Skips texts that were already analyzed with this specific tokenizer, unless instructed otherwise.
- A valid HuggingFace token might be needed to access some of the target tokenizers.

```bash
python pipeline.py analyze run-token-count --target-llm="openai/gpt-4o"
python pipeline.py analyze run-token-count --target-llm="mistralai/Mixtral-8x22B-Instruct-v0.1"
python pipeline.py analyze run-token-count --target-llm="microsoft/phi-4"
```

</details>

<details>
<summary><h3>analyze run-topic-classification</h3></summary>

Runs a topic classification model on the collection.

Notes:
- The model was trained on the data filtered by `extract-topic-classification-training-dataset`
- This command updates `TopicClassification` records
- Uses [instdin/institutional-books-topic-classifier-bert](https://huggingface.co/instdin/institutional-books-topic-classifier-bert) by default

Benchmark mode:
- Runs topic classification model on 1000 records set aside for benchmarking purposes.
- Results of the benchmark will be saved as: `/data/output/export/topic-classification-benchmark-{model-name}-{datetime}.csv`

```bash
python pipeline.py analyze run-topic-classification --benchmark-mode # 1000 benchmark entries
python pipeline.py analyze run-topic-classification # Actual classification run
python pipeline.py analyze run-topic-classification --device # Allows for specifying on which torch device the model should run
```

</details>

[👆 Back to the summary](#summary)

---

## CLI: process 

<details>
<summary><h3>ocr-postprocessing</h3></summary>

This series of commands allows for re-processing the collection's original text export. 
The goal is to attempt to make it more usable and readable, for humans and machines alike.

This process is divided in three steps (see details our technical report for more details).

### Step 01 - Generating a training dataset

This command uses at text-generation model to label line-level OCR chunks.
This data can then be used to train a (coarse) classification model, assigning a type to every chunk.

Notes:
- Pulls `--n-samples` random pages from books in `--languages`.
- Uses Ollama as an inference backend and `phi4:14b-q8_0`: make sure both are available.
- Training set is stored as `OCRPostprocessingTrainingDataset` records.
- 10% of the pages are set aside to build a test set.

```bash
python pipeline.py process ocr-postprocessing step01-generate-training-dataset
python pipeline.py process ocr-postprocessing step01-generate-training-dataset --n-samples=1000
```

### Step 02 - Train and evaluate model

This command distills a [Sentence Transformer](https://sbert.net/) model into a static embedding model via [Model2Vec](https://github.com/MinishLab/model2vec).
This model is then fine-tuned into a classifier, using the data generated in step 01.

The resulting model allows for detecting the "type" (`OCRPostprocessingTrainingDataset.TARGET_TYPE`) of each line of OCR'd text.

```bash
python pipeline process ocr-postprocessing step02-train-and-evaluate-model 
python pipeline process ocr-postprocessing step02-train-and-evaluate-model --source-model-name="sentence-transformers/LaBSE"
```

### Step 03 - Process

> ⚠️ Prototype

This command:
- Uses one of the models trained with step 02 to infer the type of each line in the original OCR export.
- Uses the detected type and heuristics to assemble the lines into more readable text.
- Outputs a single JSON file per book, handled via `BookIO.postprocessed_ocr`.

Notes:
- Whenever possible, running heads and page numbers are skipped.
- Whenever possible chunks detected as noise will be skipped (e.g: if they're only 1 character long).
- Only tested on the following languages: eng, deu, fra, ita, spa.
- This is implementation is an early prototype and is therefore more effective than efficient.

```bash
python pipeline process ocr-postprocessing step03-process --classifier-name="labse-ocr-postprocessing-2025-05-02-20-06"
```

</details>

[👆 Back to the summary](#summary)

---

## CLI: export 

<details>
<summary><h3>export stats overview</h3></summary>

Generates a single CSV with statistics from the entire pipeline.
Can be used as a "bird's eye view" of the current state of the experiments and overall dataset.

Saved as:
- `/data/output/export/overview-{datetime}.csv`

```bash
python pipeline.py export stats overview
```

</details>

<details>
<summary><h3>export misc deduplication-evaluation-sheet</h3></summary>

Exports a CSV sheet to manually evaluate the accuracy of our collection-level items deduplication method.
Randomly picks `--n-samples` samples.

Saved as:
- `/data/output/export/deduplication-eval-sheet-{n-samples}-{datetime}.csv`

```bash
python pipeline.py export misc deduplication-eval-sheet --n-samples=1000
```

</details>

<details>
<summary><h3>export misc simplified-source-metadata</h3></summary>

Simplified CSV export of the source metadata extracted from Google Books.

Saved as:
- `/data/output/export/simplified-source-metadata-{pd}-{datetime}.csv`

```bash
python pipeline.py export misc simplified-source-metadata
python pipeline.py export misc simplified-source-metadata --pd-only
```

</details>

<details>
<summary><h3>export misc topic-classification-training-set</h3></summary>

Exports the topic classification training dataset prepared via `analyze extract-topic-classification-training-dataset` as a series of CSVs.

Current setup: text classification fine-tunning
https://huggingface.co/docs/autotrain/en/text_classification

Saved as:
- `/data/output/export/topic-classification-training-dataset-{set}-{datetime}.csv`

```bash
python pipeline.py export misc topic-classification-training-dataset
```

</details>


[👆 Back to the summary](#summary)

---

## CLI: publish 

<details>
<summary><h3>publish hf generate</h3></summary>

Compiles the finalized dataset so it can be published on HuggingFace 🤗.

Notes:
- Output saved locally, in the project's data folder.
- Asks for confirmation before proceeding.
- `--include-text` allows for switching between the two versions of the dataset.
- Dataset target name is adjusted automatically.

```bash
python pipeline.py publish hf generate
python pipeline.py publish hf generate --include-text # Full dataset text_by_page_xyz fields
```

</details>

<details>
<summary><h3>publish hf push</h3></summary>

Uploads the dataset to HuggingFace 🤗.
Creates Parquet chunks of specific length and uploads them to the hub.

Notes:
- dataset.push_to_hub() cannot easily be used with this dataset (charding issues).
- Asks for confirmation before proceeding.
- `--include-text` allows for switching between the two versions of the dataset.
- Dataset target name is adjusted automatically.

```bash
python pipeline.py publish hf push
python pipeline.py publish hf push --include-text # Full dataset text_by_page_xyz fields
```

</details>

<details>
<summary><h3>publish hf check-integrity</h3></summary>

Basic integrity check for the datasets that were pushed to Hugging Face 🤗.
Compares each remote row with its local counterpart.

Notes:
- `--include-text` allows for switching between the two versions of the dataset.
- `--use-local-copy` allows for using the local copy generated with `publish hf generate`.
- Dataset target name is adjusted automatically.

```bash
python pipeline.py publish hf check-integrity
python pipeline.py publish hf check-integrity --include-text # Full dataset text_by_page_xyz fields
```

</details>

<details>
<summary><h3>HuggingFace output format</h3></summary>

#### Suffixes
| Suffix | Description
| --- | --- |
| `_src` | _"From source"_. This field's data comes from information we gathered from the collection itself. |
| `_gen` | _"Generated"_. This field's data was generated as part of our analysis / post-processing. | 
| `_ext` | _"External"_. This field's data was pulled from an external source via a records matching mechanism. |

#### Row-level fields
| Field name | Type | Description | Section in technical report |
| --- | --- | --- | --- |
| `barcode_src` | String | The volume's barcode. Serves as a primary key/identifier. | 3 |
| `title_src` | String | Merge of all the title-related bibliographic metadata available for this volume. | 3 |
| `author_src` | String | Merge of all the author name-related bibliographic metadata available for this volume. | 3 |
| `date1_src` | String | First available date for that volume. Described in `date_types_src`. May contain placeholder characters. See [MARC 21 specification](https://www.loc.gov/marc/bibliographic/bd008a.html) for details. | 4.3 |
| `date2_src` | String | Second available date for that volume. | 4.3 |
| `date_types_src` | String | Describes the nature of `date1_src` and `date2_src`. See [MARC 21 specification](https://www.loc.gov/marc/bibliographic/bd008a.html) for details. | 4.3 |
| `page_count_src` | Int | Page count for that volume. | 4.2 | 
| `token_count_o200k_base_gen` | Int | Total tokens for that volume's OCR-extracted text, as measured with `o200k_base`. | 4.2 | 
| `language_src` | String | ISO 639-3 code for the main language of this book, as expressed in the collection's bibliographic metadata. Converted from original ISO 639-2B for convenience. | 4.4 | 
| `language_gen` | String | ISO 693-3 code for the main language of this book, as detected by our text-level language analysis of the OCR-extracted text. | 4.4 | 
| `language_distribution_gen` | Dict | Distribution of the languages detected by our text-level language analysis. Only languages for which more than 1000 `o200k_base` tokens were detected in total were kept. | 4.4 | 
| `topic_or_subject_src` | String | Topic or subject information, as expressed in the collection's bibliographic metadata. Only available for (approximately) half of the collection. | 4.5 | 
| `topic_or_subject_gen` | String | High-level "topic" assigned to this volume by our [topic classification model](https://huggingface.co/instdin/institutional-books-topic-classifier-bert). Inferred from existing metadata. One of the [Library of Congress' Classification Outline](https://www.loc.gov/catdir/cpso/lcco/) first-level items. | 4.5 |
| `topic_or_subject_score_gen` | Float | Confidence score returned by our [topic classification model](TBD) for this specific prediction. | 4.5 |
| `genre_or_form_src` | String | Genre or form information, as expressed in the collection's bibliographic metadata. Only available for (approximately) 10% of the collection. | 4.5 |
| `general_note_src` | String | Additional notes about this specific volume in the collection's bibliographic metadata. | 3 |
| `ocr_score_src` | Int (0-100) | Primary OCR quality score, as expressed in the collection's metadata. | 4.7 |
| `ocr_score_gen` | Int (0-100) | Secondary OCR quality score, generated by using [pleias/OCRoscope](https://github.com/Pleias/OCRoscope) on the collection's OCR-extracted text. | 4.7 |
| `likely_duplicates_barcodes_gen` | List | List of barcodes for which the OCR-extracted text is highly-similar to this volume's. | 4.6 |
| `text_analysis_gen` | Dict | High-level text analysis of the OCR-extracted text, both original and post-processed. | 4.8 |
| `identifiers_src` | Dict | List of bibliographic identifiers, as expressed in the collection's metadata. | 3 |
| `hathitrust_data_ext` | Dict | Rights determination data pulled from the [Hathitrust API](https://www.hathitrust.org/) for this volume. | 5 |
| `text_by_page_src` | List[String] | Original OCR-extracted text for this volume. | 4.2 |
| `text_by_page_gen` | List[String] | Post-processed OCR-extracted text for this volume. Available for books in the following languages: `eng`, `deu`, `fra`, `ita`, `spa` (~850K books). | 4.9 |

#### Nested: `language_distribution_gen` fields
| Field name | Type | Description 
| --- | --- | --- |
| `languages` | List[String] | List of ISO 693-3 codes. Sorted by prevalence. |
| `proportion` | List[Float] | List of percentages. Sorted by prevalence. |

#### Nested: `text_analysis_gen` fields
| Field name | Type | Description | Section in technical report |
| --- | --- | --- | --- |
| `text_by_page_src` | Dict | Text analysis data for the original OCR-extracted text. | 4.8 |
| `text_by_page_gen` | Dict | Text analysis data for the post-processed OCR-extracted text. | 4.9 |

_Both dicts are shaped as follows when available:_

| Field name | Type | Description |
| --- | --- | --- |
| `tokenizability_score` | Float (0.0-100.0) | Measure of how close to 1.25 `o200k_base` token per word this text is. |
| `char_count` | Int | Total characters. |
| `word_count` | Int | Total detected words (language-aware tokenization). |
| `word_count_unique` | Int | Total unique detected words. |
| `word_type_token_ratio` |  Float (0.0-100.0) | Lexical diversity at word level. May help identify the underlying document type. |
| `bigram_count` | Int | Total bigrams. |
| `bigram_count_unique` | Int | Total unique bigrams. |
| `bigram_type_token_ratio` |  Float (0.0-100.0) | Lexical diversity at bigram level. May help identify the underlying document type. |
| `trigram_count` | Int | Total bigrams. |
| `trigram_count_unique` | Int | Total unique bigrams. |
| `trigram_type_token_ratio` |  Float (0.0-100.0) | Lexical diversity at bigram level. May help identify the underlying document type. |
| `sentence_count` | Int | Total detected sentences. |
| `sentence_count_unique` | Int | Total unique detected sentences. |

#### Nested: `identifiers_src` fields
| Field name | Type | Description | 
| --- | --- | --- | 
| `lccn` | List[String] | List of Library of Congress Control Numbers, if available. |
| `isbn` | List[String] | List of International Standard Book Numbers, if available. |
| `ocolc` | List[String] | List of OCLC Control Numbers, if available. |

#### Nested: `hathitrust_data_ext` fields
| Field name | Type | Description | 
| --- | --- | --- | 
| `url` | String | Permalink to that volume on Hathitrust. |
| `rights_code` | String | [Hathitrust's rights determination code](https://www.hathitrust.org/the-collection/preservation/rights-database/#:~:text=for%20open%20access-,Attributes,-Attributes). |
| `reason_code` | String | [Hathitrust's rights determination reason code](https://www.hathitrust.org/the-collection/preservation/rights-database/#:~:text=note%20for%20details-,Reasons,-Rights%20code%20reasons). |
| `last_check` | String | Date at which that information was pulled from the Hathitrust API. |
</details>

---

## Cite this work

```bibtext
@misc{cargnelutti2025institutionalbooks10242b,
      title={Institutional Books 1.0: A 242B token dataset from Harvard Library's collections, refined for accuracy and usability}, 
      author={Matteo Cargnelutti and Catherine Brobston and John Hess and Jack Cushman and Kristi Mukk and Aristana Scourtas and Kyle Courtney and Greg Leppert and Amanda Watson and Martha Whitehead and Jonathan Zittrain},
      year={2025},
      eprint={2506.08300},
      archivePrefix={arXiv},
      primaryClass={cs.CL},
      url={https://arxiv.org/abs/2506.08300}, 
}
```
</details>

[👆 Back to the summary](#summary)

---
