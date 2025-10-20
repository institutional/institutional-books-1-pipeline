import os
import multiprocessing
from pathlib import Path

import click
from datasets import Dataset, Features, Value, Sequence
from slugify import slugify
from loguru import logger

import utils
from models import BookIO
from const import HF_DATASET_DIR_PATH


@click.command("generate")
@click.option(
    "--offset",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the collection (sorted by BookIO.barcode).",
)
@click.option(
    "--limit",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the collection (sorted by BookIO.barcode).",
)
@click.option(
    "--include-text",
    type=bool,
    is_flag=True,
    default=False,
    help="If set, will include text_by_page_xyz fields.",
)
@click.option(
    "--include-non-pd",
    type=bool,
    is_flag=True,
    default=False,
    help="If set, ignores rights determination checks.",
)
@utils.needs_pipeline_ready
@utils.needs_everything
def generate(
    offset: int | None,
    limit: int | None,
    include_text: bool,
    include_non_pd: bool,
):
    """
    Compiles the finalized dataset so it can be published on HuggingFace 🤗.

    Notes:
    - Output saved locally, in the project's data folder.
    - Asks for confirmation before proceeding.
    - `--include-text` allows for switching between the two versions of the dataset.
    - Dataset target name is adjusted automatically.
    """
    from . import HF_DATASET_FEATURES, get_hf_row_from_book

    pd_only = not include_non_pd

    def gen():
        """Dataset rows generator"""
        for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():

            row = get_hf_row_from_book(
                book,
                likely_duplicates,
                pd_only=pd_only,
                include_text=include_text,
                include_hathitrust_data=include_hathitrust_data,
            )

            if not row:
                continue

            yield row

    dataset_name = (
        os.getenv("HF_DATASET_NAME_FULL") if include_text else os.getenv("HF_DATASET_NAME_METADATA")
    )

    include_hathitrust_data = os.getenv("PD_FILTERING_MECHANISM", "") == "HATHITRUST"

    #
    # Ask for confirmation
    #
    if not click.confirm(f"👀 Do you really want to prepare {dataset_name}?"):
        logger.info("Cancelled")
        exit(0)

    logger.info("Pulling list of likely duplicates ...")
    likely_duplicates = utils.get_filtered_duplicates(pd_only=pd_only)

    #
    # Compile
    #
    logger.info("Compiling dataset ...")

    features = HF_DATASET_FEATURES
    num_shards = 10_000 if include_text else None

    dataset_path = Path(HF_DATASET_DIR_PATH, slugify(dataset_name))

    if not include_text:
        del features["text_by_page_src"]
        del features["text_by_page_gen"]

    if os.getenv("PD_FILTERING_MECHANISM", "") != "HATHITRUST":
        del features["hathitrust_data_ext"]

    dataset = Dataset.from_generator(gen, features=features)

    logger.info(f"Saving HuggingFace-ready {dataset_name} dataset to disk")

    dataset.save_to_disk(
        dataset_path,
        num_proc=multiprocessing.cpu_count(),
        num_shards=num_shards,
    )

    logger.info(f"{dataset_name} saved to disk. It can now be uploaded to HuggingFace")
