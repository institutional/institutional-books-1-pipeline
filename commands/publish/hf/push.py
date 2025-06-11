import traceback
from pathlib import Path
from io import BytesIO
import math
import time

import click
from datasets import load_from_disk
from slugify import slugify
from huggingface_hub import HfApi

import utils
from models import BookIO
from const import HF_METADATA_ONLY_DATASET_NAME, HF_FULL_DATASET_NAME, OUTPUT_HF_DATASET_DIR_PATH


@click.command("push")
@click.option(
    "--include-text",
    type=bool,
    is_flag=True,
    default=False,
    help="If set, will include text_by_page_xyz fields.",
)
@click.option(
    "--skip-first-n",
    type=int,
    default=None,
    help="If set, will not push the first n samples. Helpful for resuming uploads. Zero-indexed.",
)
@utils.needs_pipeline_ready
@utils.needs_everything
def push(include_text: bool, skip_first_n: int | None):
    """
    Uploads the dataset to HuggingFace 🤗.
    Creates Parquet chunks of specific length and uploads them to the hub.

    Notes:
    - dataset.push_to_hub() cannot easily be used with this dataset (charding issues).
    - Asks for confirmation before proceeding.
    - `--include-text` allows for switching between the two versions of the dataset.
    - Dataset target name is adjusted automatically.
    """
    hf = HfApi()
    dataset_name = HF_FULL_DATASET_NAME if include_text else HF_METADATA_ONLY_DATASET_NAME
    dataset_path = Path(OUTPUT_HF_DATASET_DIR_PATH, slugify(dataset_name))

    if not click.confirm(f"👀 Do you really want to push {dataset_name} to the hub?"):
        click.echo("Cancelled.")
        exit(0)

    #
    # Load arrow data from disk
    #
    dataset = load_from_disk(dataset_path)

    #
    # Split and convert arrows into Parquet chards, upload on the fly
    #
    chunk_size = 100 if include_text else 1_000_000
    chunk_i = 0
    total_chunks = int(math.ceil(len(dataset) / chunk_size))

    click.echo(f"ℹ️ Dataset will be split into {total_chunks} Parquet chards of {chunk_size} rows.")

    for slice_i in range(0, len(dataset), chunk_size):

        if skip_first_n and chunk_i < skip_first_n:
            click.echo(f"⏩ Skipping slice {chunk_i}.")
            chunk_i += 1
            continue

        # Take slice, convert it to Parquet
        slice_ii = min(slice_i + chunk_size, len(dataset))
        chunk_bytes = BytesIO()
        chunk = dataset.select(range(slice_i, slice_ii))
        chunk.to_parquet(chunk_bytes)

        # Upload Parquet bytes
        click.echo(f"⬆️ Uploading Parquet chunk {chunk_i} (Rows {slice_i} to {slice_ii})...")
        destination = f"data/train-{str(chunk_i).zfill(5)}-of-{str(total_chunks).zfill(5)}.parquet"
        uploaded = False

        while not uploaded:
            try:
                info = hf.upload_file(
                    path_or_fileobj=chunk_bytes.getvalue(),
                    path_in_repo=destination,
                    repo_id=dataset_name,
                    repo_type="dataset",
                )

                assert info
                uploaded = True
            except Exception as err:
                click.echo(traceback.format_exc())
                click.echo(f"Failed to upload {destination}. Will retry in 1 minute ...")
                time.sleep(60)

        chunk_i += 1

    click.echo(f"✅ {dataset_name} was pushed to the hub.")
