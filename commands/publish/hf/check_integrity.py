import traceback
from pathlib import Path

import click
from slugify import slugify
from datasets import load_dataset, load_from_disk

import utils
from models import BookIO, HathitrustRightsDetermination, TokenCount
from const import (
    HF_METADATA_ONLY_DATASET_NAME,
    HF_FULL_DATASET_NAME,
    HATHITRUST_PD_CODES,
    HATHITRUST_PD_STRING,
    OUTPUT_HF_DATASET_DIR_PATH,
)


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
@utils.needs_pipeline_ready
@utils.needs_everything
def check_integrity(include_text: bool, use_local_copy: bool):
    """
    Basic integrity check for the datasets that were pushed to Hugging Face 🤗.
    Compares each remote row with its local counterpart.

    Notes:
    - `--include-text` allows for switching between the two versions of the dataset.
    - `--use-local-copy` allows for using the local copy generated with `publish hf generate`.
    - Dataset target name is adjusted automatically.
    """
    from . import get_hf_row_from_book

    dataset_name = HF_FULL_DATASET_NAME if include_text else HF_METADATA_ONLY_DATASET_NAME
    dataset = None
    likely_duplicates = None
    expected_rows_count = 0
    actual_rows_count = 0

    click.echo("🧮 Pulling list of likely duplicates ...")
    likely_duplicates = utils.get_filtered_duplicates(pd_only=True)

    if use_local_copy:
        click.echo(f"🤗 Reading {dataset_name} from disk ...")
        dataset = load_from_disk(Path(OUTPUT_HF_DATASET_DIR_PATH, slugify(dataset_name)))
    else:
        click.echo(f"🤗 Streaming {dataset_name} from HuggingFace ...")
        dataset = load_dataset(dataset_name, split="train", streaming=True)

    #
    # Rows count must match current count of PD rows with text (>100 tokens) in database
    #
    expected_rows_count = (
        HathitrustRightsDetermination.select()
        .where(
            HathitrustRightsDetermination.rights_code.in_(HATHITRUST_PD_CODES),
            HathitrustRightsDetermination.us_rights_string == HATHITRUST_PD_STRING,
            HathitrustRightsDetermination.book.in_(
                TokenCount.select(TokenCount.book).where(
                    TokenCount.target_llm == "openai/gpt-4o",
                    TokenCount.count > 100,
                )
            ),
        )
        .count()
    )

    #
    # Row-by-row surface-level check
    #
    for row in dataset:

        click.echo(f"🔍 Checking row {actual_rows_count}")
        actual_rows_count += 1

        try:
            book = BookIO.get(barcode=row["barcode_src"])

            row_check = get_hf_row_from_book(
                book,
                likely_duplicates,
                pd_only=True,
                include_text=include_text,
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
            click.echo(traceback.format_exc())
            click.echo(f"🛑 {dataset_name} Mismatch for row {row['barcode_src']}!")
            exit(1)

    # Row count check
    try:
        assert actual_rows_count == expected_rows_count
    except Exception:
        click.echo(
            f"🛑 {dataset_name}: rows count mismatch. "
            + f"Expected {expected_rows_count} got {actual_rows_count}."
        )
        exit(1)

    if use_local_copy:
        click.echo(f"✅ (Local) {dataset_name} passed the integrity check.")
    else:
        click.echo(f"✅ (Remote) {dataset_name} matches with local data.")
