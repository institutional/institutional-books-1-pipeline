import csv
from pathlib import Path
import random

import click
import peewee

import utils
from models import BookIO, ScannedTextSimhash
from const import OUTPUT_MISC_DIR_PATH, DATETIME_SLUG


@click.command("step02-export-simhash-eval-sheet")
@click.option(
    "--n-samples",
    type=int,
    required=False,
    default=100,
    help="Number of items to pull for validation.",
)
@utils.needs_pipeline_ready
def step02_export_simhash_eval_sheet(n_samples: int):
    """
    Collection-level items deduplication, step 02:
    Generates and exports an evaluation sheet for manual review.
    This sheet can be used to control how well simhash performed at detecting near-duplicate scanned texts.

    Notes:
    - Samples are grouped by identical simhash. Therefore n samples = n hashes, and the barcodes they match with.

    Saved as:
    - `/data/output/misc/deduplication-simhash-eval-sheet-{n-samples}-{datetime}.csv`
    """
    output_filepath = Path(
        OUTPUT_MISC_DIR_PATH,
        f"deduplication-simhash-eval-sheet-{n_samples}-{DATETIME_SLUG}.csv",
    )

    hashes_to_barcodes = {}

    #
    # Collect group of identical barcodes
    #
    # Note: table is shuffled, hashes are written in dict in order of appeareance: dict is reasonably shuffled.
    click.echo("📋 Collecting simhash to barcodes mappings ...")
    for entry in ScannedTextSimhash.select().order_by(peewee.fn.Random()).iterator():
        hash = entry.hash
        barcode = entry.book.barcode

        if hash is None:
            continue

        if hashes_to_barcodes.get(hash, None) is None:
            hashes_to_barcodes[hash] = []

        hashes_to_barcodes[hash].append(barcode)

    #
    # TODO: Export samples
    #
    click.echo(f"💾 Saving {n_samples} samples ...")
    for i, entry in enumerate(hashes_to_barcodes.items(), start=1):
        if i >= n_samples:
            break

        # WIP

    click.echo(f"✅ {output_filepath.name} saved to disk.")
