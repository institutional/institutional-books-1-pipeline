import os
import re
import gzip
import shutil
from pathlib import Path
import traceback
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


@click.command("build")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
)
@click.option(
    "--tables-only",
    is_flag=True,
    default=False,
    help="If set, will only create tables and stop.",
)
def build(overwrite: bool, tables_only: bool):
    """
    Prepares the pipeline:
    - Creates database tables
    - Downloads and indexes source files from the output of GRIN-TO-S3 that was saved on S3/R2.
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
    with ProcessPoolExecutor(max_workers=16) as executor:
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
