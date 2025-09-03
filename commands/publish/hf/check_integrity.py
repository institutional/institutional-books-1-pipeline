import os
import traceback
from pathlib import Path

import click
from slugify import slugify
from datasets import load_dataset, load_from_disk
from loguru import logger

import utils
from models import BookIO
from const import HF_DATASET_DIR_PATH


@click.command("check-integrity")
@click.option(
    "--use-local-copy",
    type=bool,
    is_flag=True,
    default=False,
    help="If set, will load the dataset from disk.",
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
def check_integrity(use_local_copy: bool, include_text: bool, include_non_pd: bool):
    """
    Basic integrity check for the datasets that were pushed to Hugging Face 🤗.
    Compares each remote row with its local counterpart.

    Notes:
    - `--include-text` allows for switching between the two versions of the dataset.
    - `--use-local-copy` allows for using the local copy generated with `publish hf generate`.
    - Dataset target name is adjusted automatically.
    """
    from . import get_hf_row_from_book

    pd_only = not include_non_pd

    dataset_name = (
        os.getenv("HF_DATASET_NAME_FULL") if include_text else os.getenv("HF_DATASET_NAME_METADATA")
    )

    dataset = None
    likely_duplicates = None

    logger.info("Pulling list of likely duplicates ...")
    likely_duplicates = utils.get_filtered_duplicates(pd_only=pd_only)

    if use_local_copy:
        logger.info(f"Reading {dataset_name} from disk ...")
        dataset = load_from_disk(Path(HF_DATASET_DIR_PATH, slugify(dataset_name)))
    else:
        logger.info(f"Streaming {dataset_name} from HuggingFace ...")
        dataset = load_dataset(dataset_name, split="train", streaming=True)

    #
    # Row-by-row surface-level check
    #
    include_hathitrust_data = os.getenv("PD_FILTERING_MECHANISM", "") == "HATHITRUST"

    for row in dataset:

        logger.info(f"Checking row {actual_rows_count}")
        actual_rows_count += 1

        try:
            book = BookIO.get(barcode=row["barcode_src"])

            row_check = get_hf_row_from_book(
                book,
                likely_duplicates,
                pd_only=pd_only,
                include_text=include_text,
                include_hathitrust_data=include_hathitrust_data,
            )

            for key in row.keys():
                # `language_distribution_gen` is a special case: Arrow converts it from dicts to arrays
                if key == "language_distribution_gen":
                    assert row["language_distribution_gen"]["language"] == [
                        entry["language"] for entry in row_check["language_distribution_gen"]
                    ]

                    assert row["language_distribution_gen"]["proportion"] == [
                        entry["proportion"] for entry in row_check["language_distribution_gen"]
                    ]
                else:
                    assert row[key] == row_check[key]
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.error(f"{dataset_name} Mismatch for row {row['barcode_src']}!")
            exit(1)

    if use_local_copy:
        logger.info(f"(Local) {dataset_name} passed the integrity check.")
    else:
        logger.info(f"(Remote) {dataset_name} matches with local data.")
