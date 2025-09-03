import multiprocessing
import csv
from pathlib import Path
import random

import click
from loguru import logger

import utils
from models import BookIO, ScannedTextSimhash
from const import EXPORT_DIR_PATH, DATETIME_SLUG


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
@utils.needs_scanned_text_simhash_data
@utils.needs_text_analysis_data
@utils.needs_language_detection_data
def deduplication_evaluation_sheet(n_samples: int, max_workers: int):
    """
    Exports a CSV sheet to manually evaluate the accuracy of our collection-level items deduplication method.
    Randomly picks `--n-samples` samples.

    Saved as:
    - `/data/output/export/deduplication-eval-sheet-{n-samples}-{datetime}.csv`
    """
    hashes_to_books = {}
    hashes_to_books_sample = {}

    #
    # Collect group of duplicates
    #
    logger.info("Collecting likely duplicates ...")
    hashes_to_books = utils.get_filtered_duplicates(max_workers)

    # Pick `n_samples` hashes (quadruple `n_samples` to account for export filters)
    hashes_to_books_sample = list(hashes_to_books.keys())
    random.shuffle(hashes_to_books_sample)
    hashes_to_books_sample = hashes_to_books_sample[0 : int(n_samples * 4)]

    #
    # Export samples
    #
    logger.info(f"Saving {n_samples} samples ...")

    output_filepath = Path(
        EXPORT_DIR_PATH,
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

            writer.writerow([simhash] + gbooks_urls)

            samples_written += 1

    logger.info(f"{output_filepath.name} saved to disk ({n_samples} samples)")
