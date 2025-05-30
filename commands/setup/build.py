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
def build(
    overwrite: bool,
    tables_only: bool,
    max_parallel_downloads: int,
    max_workers: int,
):
    """
    Initializes the pipeline:
    - Creates the local database and its tables
    - Downloads source files from the output of `grin-to-s3`, hosted on S3 or R2
    - Indexes records within individual CSV and JSONL files so `BookIO` can perform fast random access on any barcode.
    """
    jsonl_gz_remote_filepaths = []

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
