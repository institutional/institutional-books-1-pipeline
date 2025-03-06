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

    hashes_to_books = {}

    #
    # Collect group of identical barcodes
    #
    # Note: table is shuffled, hashes are written in dict in order of appeareance: dict is reasonably shuffled.
    click.echo("📋 Collecting simhash to barcodes mappings ...")
    for entry in ScannedTextSimhash.select().order_by(peewee.fn.Random()).iterator():
        hash = entry.hash
        book = entry.book

        if hash is None:
            continue

        if hashes_to_books.get(hash, None) is None:
            hashes_to_books[hash] = []

        hashes_to_books[hash].append(book)

    #
    # Export samples
    #
    click.echo(f"💾 Saving {n_samples} samples ...")

    with open(output_filepath, "w+") as fd:
        samples_written = 0

        writer = csv.writer(fd)

        # Headers = simhash, gbooks_url_{1...20}
        writer.writerow(["simhash"] + [f"gbooks_url_{i}" for i in range(1, 21)])

        for simhash, books in hashes_to_books.items():
            gbooks_urls = []

            if samples_written >= n_samples:
                break

            # Focus on items that have at least 1 likely duplicate
            if len(books) < 2:
                continue

            # Check that all barcodes are in the "VIEW_FULL" tranche.
            # This will help review items by eliminating entries that can't be checked online.
            not_all_view_full = False

            for book in books:
                if book.tranche != "VIEW_FULL":
                    not_all_view_full = True

                gbooks_urls.append(book.csv_data["Google Books Link"])

            if not_all_view_full:
                continue

            writer.writerow([simhash] + gbooks_urls)

            samples_written += 1

    click.echo(f"✅ {output_filepath.name} saved to disk.")

    #
    # Print stats
    #
    total_books = ScannedTextSimhash.select().count()

    total_books_with_hash = (
        ScannedTextSimhash.select().where(ScannedTextSimhash.hash.is_null(False)).count()
    )

    total_unique_books = len(hashes_to_books.keys())
    total_unique_books_with_duplicates = 0
    total_duplicate_books = 0

    for simhash, books in hashes_to_books.items():
        if len(books) > 1:
            total_unique_books_with_duplicates += 1
            total_duplicate_books += len(books)

    click.echo(f"📊 Books: {total_books}")
    click.echo(f"📊 Books w/ text and simhash: {total_books_with_hash}")
    click.echo(f"📊 Unique books: {total_unique_books}")
    click.echo(f"📊 Unique books w/ at least 1 duplicate: {total_unique_books_with_duplicates}")
    click.echo(f"📊 Duplicate books: {total_duplicate_books}")
