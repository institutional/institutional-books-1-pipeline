import os
import csv
import json
import gzip
import multiprocessing
import shutil
from pathlib import Path
import traceback
import glob
from concurrent.futures import ProcessPoolExecutor, as_completed

import click
from datasets import load_dataset

import utils
import models
from models import BookIO
from const import (
    TABLES,
    INPUT_JSONL_DIR_PATH,
    INPUT_CSV_DIR_PATH,
    GRIN_TO_S3_TRANCHES,
    GRIN_TO_S3_TRANCHES_TO_BUCKET_NAMES,
    GRIN_TO_S3_BUCKET_NAMES_TO_TRANCHES,
    GRIN_TO_S3_BUCKET_VERSION_PREFIX,
)
import utils.pipeline_readiness


@click.command("build")
@click.option(
    "--source",
    type=click.Choice(['s3', 'hf']),
    default='s3',
    help="Data source: 's3' for original S3 buckets, 'hf' for HuggingFace dataset",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing files.",
)
@click.option(
    "--tables-only",
    is_flag=True,
    default=False,
    help="If set, will only create tables and stop.",
)
@click.option(
    "--max-parallel-downloads",
    type=int,
    default=16,
    help="Determines how many files can be downloaded in parallel.",
)
@click.option(
    "--max-workers",
    type=int,
    default=multiprocessing.cpu_count(),
    help="Determines how many workers can be spun up for multiprocessing tasks.",
)
@click.option(
    "--limit",
    type=int,
    help="Limit number of books to download (useful for HF testing)",
)
def build(
    source: str,
    overwrite: bool,
    tables_only: bool,
    max_parallel_downloads: int,
    max_workers: int,
    limit: int = None,
):
    """
    Initializes the pipeline:
    - Creates the local database and its tables
    - Downloads source files from S3 buckets OR HuggingFace dataset
    - Indexes records within individual CSV and JSONL files so `BookIO` can perform fast random access on any barcode.
    """
    #
    # Database setup
    #
    click.echo("🗄️ Setting up the database ...")

    with utils.get_db() as db:
        try:
            db.create_tables(
                [models.__getattribute__(model_name) for model_name in TABLES.values()]
            )

            if tables_only:
                exit(0)

        except Exception:
            click.echo(traceback.format_exc())
            click.echo("Could not initialize database.")
            exit(1)

    # Route to appropriate build method based on source
    if source == 'hf':
        click.echo("🤗 Building from HuggingFace dataset...")
        build_from_huggingface(limit, max_workers, overwrite)
        
        # Index the HF files for compatibility with existing pipeline
        click.echo("📋 Indexing HF content for pipeline compatibility...")
        index_hf_files(max_workers)
    else:
        click.echo("☁️ Building from S3 buckets...")
        build_from_s3(overwrite, max_parallel_downloads, max_workers)

    # Mark pipeline as ready
    utils.pipeline_readiness.set_pipeline_readiness(True)
    click.echo("✅ Pipeline is ready.")


def build_from_s3(overwrite: bool, max_parallel_downloads: int, max_workers: int):
    """Original S3-based build process"""
    jsonl_gz_remote_filepaths = []

    #
    # List available jsonl.gz files for each bucket
    #
    click.echo("📋 Listing available jsonl.gz files for all tranches ...")

    for bucket_name in GRIN_TO_S3_BUCKET_NAMES_TO_TRANCHES.keys():
        filepaths = list_remote_jsonl_gz_files(bucket_name)
        jsonl_gz_remote_filepaths = jsonl_gz_remote_filepaths + filepaths

    click.echo(f"-- Found {len(jsonl_gz_remote_filepaths)} jsonl.gz files.")

    #
    # Download and unpack jsonl.gz files, in parallel
    #
    with ProcessPoolExecutor(max_workers=max_parallel_downloads) as executor:
        futures = []

        for remote_filepath in jsonl_gz_remote_filepaths:
            target_bucket_name = ""

            # Re-associate bucket name based on tranche, which can be found in the filename
            for tranche, bucket_name in GRIN_TO_S3_TRANCHES_TO_BUCKET_NAMES.items():
                if tranche in remote_filepath:
                    target_bucket_name = bucket_name
                    break

            batch = executor.submit(
                pull_and_unpack_jsonl_gz_file,
                target_bucket_name,
                remote_filepath,
                overwrite,
            )

            futures.append(batch)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not download or unpack jsonl.gz file. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)

    #
    # Download CSV files
    #
    for tranche, bucket_name in GRIN_TO_S3_TRANCHES_TO_BUCKET_NAMES.items():
        try:
            pull_csv_file(tranche, bucket_name, overwrite)
        except Exception:
            click.echo(traceback.format_exc())
            click.echo("Could not download books.csv file. Interrupting.")
            exit(1)

    #
    # Download latest Hathifiles (rights determination)
    #

    #
    # Index jsonl files (parallelized)
    #
    click.echo("📋 Indexing content from individual JSONL files ...")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for jsonl_filepath in glob.glob(f"{INPUT_JSONL_DIR_PATH}/*.jsonl"):
            target_bucket_name = ""

            batch = executor.submit(index_jsonl_file, Path(jsonl_filepath))

            futures.append(batch)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not index contents of JSONL file. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)

    #
    # Index CSV records
    #
    click.echo("📋 Indexing content from individual CSV files ...")

    # NOTE: Very basic parallelization.
    # This could be much faster - but doesn't matter much in that case.
    with ProcessPoolExecutor(max_workers=len(GRIN_TO_S3_TRANCHES)) as executor:
        futures = []

        for tranche in GRIN_TO_S3_TRANCHES:
            batch = executor.submit(index_csv_file, tranche)
            futures.append(batch)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not index contents of CSV file. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)

    #
    # Mark pipeline as ready
    #
    utils.pipeline_readiness.set_pipeline_readiness(True)
    click.echo("✅ Pipeline is ready.")


def list_remote_jsonl_gz_files(bucket_name: str) -> list[str]:
    """
    Lists available json.gz file for a given bucket.
    On R2, these files are stored as follows:
    - /v2/jsonl/{tranche}-0001.jsonl.gz
    """
    prefix = f"{GRIN_TO_S3_BUCKET_VERSION_PREFIX}/jsonl/"
    more_files_to_list = True
    start_after = ""

    filepaths = []

    iterations = 0
    max_iterations = 100

    while more_files_to_list and iterations < max_iterations:
        response = utils.get_s3_client().list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            StartAfter=start_after,
            MaxKeys=1000,
        )

        # Only consider files that:
        # - are .jsonl.gz
        # - a prefixed with the name of one of the tranches
        for item in response.get("Contents"):
            key = item["Key"]

            if (
                not key.startswith(prefix)
                or Path(key).name.split("-")[0] not in GRIN_TO_S3_TRANCHES
                or not key.endswith(".jsonl.gz")
            ):
                continue

            filepaths.append(item["Key"])

        # Stop whenever we stop getting a response object or "IsTruncated" is False
        if not response or response.get("IsTruncated") != True:
            more_files_to_list = False

        # StartAfter is used for pagination. Resume after last file of current "page"
        start_after = filepaths[-1]
        iterations += 1

    return filepaths


def pull_and_unpack_jsonl_gz_file(
    bucket_name: str,
    remote_jsonl_gz_filepath: str,
    overwrite: bool = False,
) -> bool:
    """
    Pulls a remote jsonl.gz file and unpacks it.
    """
    jsonl_gz_filename = Path(remote_jsonl_gz_filepath).name
    jsonl_filename = jsonl_gz_filename.replace(".gz", "")

    local_jsonl_gz_filepath = Path(INPUT_JSONL_DIR_PATH, jsonl_gz_filename)
    local_jsonl_filepath = Path(INPUT_JSONL_DIR_PATH, jsonl_filename)

    # Check if file already exists if overwrite is False
    if overwrite is False and local_jsonl_filepath.exists() and local_jsonl_filepath.stat().st_size:
        click.echo(f"⏭️ Skipping {jsonl_gz_filename} (already present + no overwrite)")
        return

    # Download
    click.echo(f"⬇️ Downloading {jsonl_gz_filename}")

    utils.get_s3_client().download_file(
        Bucket=bucket_name,
        Key=remote_jsonl_gz_filepath,
        Filename=local_jsonl_gz_filepath,
    )

    # Unpack
    click.echo(f"🗃️ Unpacking {jsonl_gz_filename} as {jsonl_filename}")

    with gzip.open(local_jsonl_gz_filepath, "rb") as fd_in:
        with open(local_jsonl_filepath, "wb") as fd_out:
            shutil.copyfileobj(fd_in, fd_out)

    # Remove .gz file
    os.unlink(local_jsonl_gz_filepath)

    return True


def pull_csv_file(tranche: str, bucket_name: str, overwrite: bool = False) -> bool:
    """
    Pulls a {tranche}-book.csv file.
    """
    remote_csv_filepath = Path(
        GRIN_TO_S3_BUCKET_VERSION_PREFIX,
        "csv",
        f"{tranche}-books.csv",
    )

    local_csv_filepath = Path(
        INPUT_CSV_DIR_PATH,
        remote_csv_filepath.name,
    )

    # Check if file already exists if overwrite is False
    if overwrite is False and local_csv_filepath.exists() and local_csv_filepath.stat().st_size:
        click.echo(f"⏭️ Skipping {local_csv_filepath.name} (already present + no overwrite)")
        return

    click.echo(f"⬇️ Downloading {local_csv_filepath.name}")

    utils.get_s3_client().download_file(
        Bucket=bucket_name,
        Key=str(remote_csv_filepath),
        Filename=local_csv_filepath,
    )

    return True


def index_jsonl_file(jsonl_filepath: Path) -> bool:
    """
    Creates (or replaces) BookIO records for a given jsonl file.
    Keeps track of the offset of each JSON record to allow for random access.
    """
    entries_to_update = []
    entries_to_create = []

    #
    # Iterate over file to collect records
    #
    file_row = 0  # Serves as index and counter
    file_errors = 0
    offset = 0

    with open(jsonl_filepath, "r+") as jsonl_file:
        while True:
            try:
                line = jsonl_file.readline()
                data = None
                entry = None

                # EOF
                if not line:
                    break

                # Parse JSON data from line (validity check)
                data = json.loads(line)
                assert data

                # Check if record exists for this barcode, create it otherwise
                try:
                    entry = BookIO.get(barcode=data["barcode"])
                    entries_to_update.append(entry)
                except:
                    entry = BookIO()
                    entry.barcode = data["barcode"]
                    entries_to_create.append(entry)

                # Extract tranche from filename: {VIEW_FULL}-0001.jsonl
                entry.tranche = jsonl_filepath.name.split("-")[0]  # {VIEW_FULL}-0001.jsonl

                if entry.tranche not in GRIN_TO_S3_TRANCHES:
                    raise Exception(f"{entry.tranche} is not a valid tranche.")

                # Extract file number from filename: VIEW_FULL-{0001}.jsonl
                entry.jsonl_file_number = int(
                    jsonl_filepath.name.replace(".jsonl", "").split("-")[1]
                )

                if entry.jsonl_file_number is None or entry.jsonl_file_number < 0:
                    raise Exception(f"{entry.jsonl_file_number} is not a valid file number.")

                entry.jsonl_offset = offset

                file_row += 1
            except Exception:
                click.echo(traceback.format_exc())
                click.echo(f"⏭️ {jsonl_filepath}: Error in row #{file_row}. Skipping.")
                file_errors += 1
                file_row += 1
            # In any case: move cursors to next line
            finally:
                offset = jsonl_file.tell()

    #
    # Create / update records in bulk
    #
    if entries_to_update:
        click.echo(f"💾 {len(entries_to_update)} records to update from {jsonl_filepath.name}")
        with utils.get_db().atomic():
            BookIO.bulk_update(
                entries_to_update,
                fields=[BookIO.tranche, BookIO.jsonl_file_number, BookIO.jsonl_offset],
                batch_size=1000,
            )

    if entries_to_create:
        click.echo(f"💾 {len(entries_to_create)} records to create from {jsonl_filepath.name}")
        with utils.get_db().atomic():
            BookIO.bulk_create(entries_to_create, batch_size=1000)


def index_csv_file(tranche: str) -> bool:
    """
    Updates BookIO records to track the offset of each CSV record to allow for random access.
    """
    entries_to_update = []

    csv_filepath = Path(INPUT_CSV_DIR_PATH, f"{tranche}-books.csv")

    with open(csv_filepath, "r+") as csv_file:
        csv_file.readline()  # Skip first line (headers)

        while True:
            csv_offset = csv_file.tell()
            csv_line = csv_file.readline()

            if not csv_line:  # EOF
                break

            csv_data = csv.reader([csv_line]).__next__()
            barcode = csv_data[0]

            # Load database record for that barcode and save CSV offset
            entry = BookIO.get(barcode=barcode)
            entry.csv_offset = csv_offset

            entries_to_update.append(entry)

    if entries_to_update:
        click.echo(f"💾 {len(entries_to_update)} records to update from {csv_filepath.name}")
        with utils.get_db().atomic():
            BookIO.bulk_update(entries_to_update, fields=[BookIO.csv_offset], batch_size=1000)


def build_from_huggingface(limit: int = None, max_workers: int = None, overwrite: bool = False):
    """
    Build local database from HuggingFace dataset instead of S3.
    Creates proper file structure compatible with existing pipeline.
    """
    dataset_name = "instdin/institutional-books-1.0"
    
    # Ensure directories exist
    os.makedirs(INPUT_JSONL_DIR_PATH, exist_ok=True)
    os.makedirs(INPUT_CSV_DIR_PATH, exist_ok=True)

    # Load and process dataset
    click.echo(f"📚 Loading dataset from HuggingFace...")
    try:
        dataset = load_dataset(dataset_name, split="train", streaming=True)
    except Exception as e:
        click.echo(f"❌ Could not load dataset: {e}")
        return

    # Create file structure compatible with pipeline
    tranche = "hf_data"
    jsonl_filepath = Path(INPUT_JSONL_DIR_PATH, f"{tranche}-0001.jsonl")
    csv_filepath = Path(INPUT_CSV_DIR_PATH, f"{tranche}-books.csv")
    
    # Remove existing files if overwrite is enabled
    if overwrite:
        if jsonl_filepath.exists():
            jsonl_filepath.unlink()
        if csv_filepath.exists():
            csv_filepath.unlink()
    
    # Skip if files exist and overwrite is False
    if not overwrite and jsonl_filepath.exists() and csv_filepath.exists():
        click.echo(f"⏭️ HF files already exist. Use --overwrite to regenerate.")
        return
    
    click.echo("🔄 Creating compatible file structure...")
    
    books_processed = 0
    csv_header_written = False
    
    with open(jsonl_filepath, 'w') as jsonl_file, open(csv_filepath, 'w', newline='') as csv_file:
        csv_writer = None
        
        for row in dataset:
            if limit and books_processed >= limit:
                break
            
            # Create JSONL entry (compatible with existing pipeline)
            jsonl_data = create_jsonl_entry_from_hf_row(row)
            if not jsonl_data:
                continue
                
            # Write to JSONL file
            jsonl_offset = jsonl_file.tell()
            json.dump(jsonl_data, jsonl_file)
            jsonl_file.write('\n')
            
            # Create CSV entry (for metadata extraction) - always create one
            csv_data = create_csv_entry_from_hf_row(row)
            if not csv_data:
                # Create minimal CSV entry if data is missing
                csv_data = {
                    'Barcode': row.get('barcode_src', ''),
                    'Title': '',
                    'Author': '',
                    'Language': '',
                    'Date 1': '',
                    'Date 2': '',
                    'Page Count': 0,
                    'OCR Analysis Score': 0,
                    'Topic or Subject': '',
                    'Genre or Form': '',
                    'Publisher': '',
                    'Place of Publication': '',
                }
            
            if not csv_header_written:
                csv_writer = csv.DictWriter(csv_file, fieldnames=csv_data.keys())
                csv_writer.writeheader()
                csv_header_written = True
            
            csv_offset = csv_file.tell()
            csv_writer.writerow(csv_data)
            
            # BookIO records will be created later by indexing process
            
            books_processed += 1
            if books_processed % 100 == 0:
                click.echo(f"📚 Processed {books_processed} books...")

    click.echo(f"🎉 Built database with {books_processed} books from HuggingFace!")
    click.echo(f"📁 Created {jsonl_filepath.name} and {csv_filepath.name}")


def create_jsonl_entry_from_hf_row(hf_row):
    """
    Create JSONL entry compatible with existing pipeline from HF row.
    """
    barcode = hf_row.get('barcode_src')
    if not barcode:
        return None
    
    # Create JSONL data structure that matches S3 format
    return {
        'barcode': barcode,
        'title': hf_row.get('title_src', ''),
        'author': hf_row.get('author_src', ''),
        'text_by_page': hf_row.get('text_by_page_src', []),
        'text_by_page_gen': hf_row.get('text_by_page_gen', []),
        'language': hf_row.get('language_src', ''),
        'language_gen': hf_row.get('language_gen', ''),
        'page_count': hf_row.get('page_count_src', 0),
        'token_count': hf_row.get('token_count_o200k_base_gen', 0),
        'ocr_score': hf_row.get('ocr_score_src', 0),
        'ocr_score_gen': hf_row.get('ocr_score_gen', 0),
        'date1': hf_row.get('date1_src', ''),
        'date2': hf_row.get('date2_src', ''),
        'topic_or_subject': hf_row.get('topic_or_subject_src', ''),
        'genre_or_form': hf_row.get('genre_or_form_src', ''),
        'identifiers': hf_row.get('identifiers_src', {}),
        'hathitrust_data': hf_row.get('hathitrust_data_ext', {}),
    }


def create_csv_entry_from_hf_row(hf_row):
    """
    Create CSV entry compatible with metadata extraction from HF row.
    """
    barcode = hf_row.get('barcode_src')
    if not barcode:
        return None
    
    # Create CSV data structure that matches expected metadata format
    return {
        'Barcode': barcode,
        'Title': hf_row.get('title_src', ''),
        'Author': hf_row.get('author_src', ''),
        'Language': hf_row.get('language_src', ''),
        'Date 1': hf_row.get('date1_src', ''),
        'Date 2': hf_row.get('date2_src', ''),
        'Page Count': hf_row.get('page_count_src', 0),
        'OCR Analysis Score': hf_row.get('ocr_score_src', 0),
        'Topic or Subject': hf_row.get('topic_or_subject_src', ''),
        'Genre or Form': hf_row.get('genre_or_form_src', ''),
        'Publisher': hf_row.get('publisher_src', ''),
        'Place of Publication': hf_row.get('place_of_publication_src', ''),
    }




def index_hf_files(max_workers: int):
    """
    Index HF files using the same logic as S3 files for pipeline compatibility.
    """
    tranche = "hf_data"
    
    # Index JSONL file
    jsonl_filepath = Path(INPUT_JSONL_DIR_PATH, f"{tranche}-0001.jsonl")
    if jsonl_filepath.exists():
        click.echo(f"📋 Indexing {jsonl_filepath.name}...")
        index_jsonl_file(jsonl_filepath)
    
    # Index CSV file
    csv_filepath = Path(INPUT_CSV_DIR_PATH, f"{tranche}-books.csv") 
    if csv_filepath.exists():
        click.echo(f"📋 Indexing {csv_filepath.name}...")
        index_csv_file(tranche)
