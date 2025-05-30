import multiprocessing
from pathlib import Path

import click
from datasets import Dataset, Features, Value, Sequence
from slugify import slugify

import utils
from models import BookIO
from const import HF_METADATA_ONLY_DATASET_NAME, HF_FULL_DATASET_NAME, OUTPUT_HF_DATASET_DIR_PATH


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
@utils.needs_pipeline_ready
@utils.needs_everything
def generate(
    offset: int | None,
    limit: int | None,
    include_text: bool,
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

    def gen():
        """Dataset rows generator"""
        for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():

            row = get_hf_row_from_book(
                book,
                likely_duplicates,
                pd_only=True,
                include_text=include_text,
            )

            if not row:
                continue

            yield row

    dataset_name = HF_FULL_DATASET_NAME if include_text else HF_METADATA_ONLY_DATASET_NAME

    #
    # Ask for confirmation
    #
    if not click.confirm(f"👀 Do you really want to prepare {dataset_name}?"):
        click.echo("Cancelled.")
        exit(0)

    click.echo("🧮 Pulling list of likely duplicates ...")
    likely_duplicates = utils.get_filtered_duplicates(pd_only=True)

    #
    # Compile
    #
    click.echo("🧮 Compiling dataset ...")

    features = HF_DATASET_FEATURES
    num_shards = 10_000 if include_text else None

    dataset_path = Path(OUTPUT_HF_DATASET_DIR_PATH, slugify(dataset_name))

    if not include_text:
        del features["text_by_page_src"]
        del features["text_by_page_gen"]

    dataset = Dataset.from_generator(gen, features=features)

    click.echo(f"🤗 Saving HuggingFace-ready {dataset_name} dataset to disk.")
    dataset.save_to_disk(dataset_path, num_proc=multiprocessing.cpu_count(), num_shards=num_shards)
    click.echo(f"✅ {dataset_name} saved to disk. It can now be uploaded to HuggingFace.")
