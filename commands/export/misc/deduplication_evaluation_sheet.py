import multiprocessing
import csv
from pathlib import Path
import random

import click

import utils
from models import BookIO, ScannedTextSimhash
from const import OUTPUT_EXPORT_DIR_PATH, DATETIME_SLUG


@click.command("deduplication-evaluation-sheet")
@click.option(
    "--n-samples",
    type=int,
    required=False,
    default=100,
    help="Number of items to pull for validation.",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=multiprocessing.cpu_count(),
    help="Determines how many subprocesses can be run in parallel.",
)
@utils.needs_pipeline_ready
def deduplication_evaluation_sheet(n_samples: int, max_workers: int):
    """
    Exports a CSV sheet to evaluate the accuracy of our collection-level items deduplication method.

    Saved as:
    - `/data/output/export/deduplication-eval-sheet-{n-samples}-{datetime}.csv`
    - `/data/output/export/deduplication-eval-sheet-stats-{datetime}.csv`
    """
    hashes_to_books = {}
    hashes_to_books_sample = {}

    #
    # Collect group of duplicates
    #
    click.echo("📋 Collecting likely duplicates ...")
    hashes_to_books = utils.get_filtered_duplicates(max_workers)

    # Pick `n_samples` hashes (quadruple `n_samples` to account for export filters)
    hashes_to_books_sample = list(hashes_to_books.keys())
    random.shuffle(hashes_to_books_sample)
    hashes_to_books_sample = hashes_to_books_sample[0 : int(n_samples * 4)]

    #
    # Export samples
    #
    click.echo(f"💾 Saving {n_samples} samples ...")

    output_filepath = Path(
        OUTPUT_EXPORT_DIR_PATH,
        f"deduplication-eval-sheet-{n_samples}-{DATETIME_SLUG}.csv",
    )

    with open(output_filepath, "w+") as fd:
        writer = csv.writer(fd)
        samples_written = 0

        # Headers = simhash, gbooks_url_{1...20}
        writer.writerow(["simhash"] + [f"gbooks_url_{i}" for i in range(1, 21)])

        for simhash in hashes_to_books_sample:
            books = hashes_to_books[simhash]
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

    click.echo(f"✅ {output_filepath.name} saved to disk ({n_samples} samples).")

    #
    # Export stats
    #
    click.echo(f"💾 Saving stats ...")

    output_filepath = Path(
        OUTPUT_EXPORT_DIR_PATH,
        f"deduplication-eval-sheet-stats-{DATETIME_SLUG}.csv",
    )

    total_books = BookIO.select().count()

    total_books_with_simhash = (
        ScannedTextSimhash.select().where(ScannedTextSimhash.hash.is_null(False)).count()
    )

    total_unique_books_with_duplicates = 0
    total_duplicate_books = 0

    for simhash, books in hashes_to_books.items():
        if len(books) > 1:
            total_unique_books_with_duplicates += 1
            total_duplicate_books += len(books)

    with open(output_filepath, "w+") as fd:
        writer = csv.writer(fd)

        writer.writerow(["Total books", total_books])

        writer.writerow(["Total books with text", total_books_with_simhash])

        writer.writerow(
            [
                "Total books with text and at least one duplicate (inclusive)",
                total_duplicate_books,
            ]
        )

        writer.writerow(
            [
                "Total unique books with text in duplicate set",
                total_unique_books_with_duplicates,
            ]
        )

        writer.writerow(
            [
                "Total unique books with text in collection",
                total_books_with_simhash
                - total_duplicate_books
                + total_unique_books_with_duplicates,
            ]
        )

    click.echo(f"✅ {output_filepath.name} saved to disk.")
