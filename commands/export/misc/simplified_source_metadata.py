from pathlib import Path
import csv
import os

import click
from loguru import logger

import utils
from const import EXPORT_DIR_PATH, DATETIME_SLUG
from models import BookIO

FIELDS_TO_EXPORT = [
    "Barcode",
    "Google Books Link",
    # Only addition: Hathitrust link
    "Hathitrust Link",
    # MARC
    "MARC Control Number",
    "MARC Date 1",
    "MARC Date 2",
    "MARC Date Type",
    "MARC Language",
    "MARC LCCN",
    "MARC ISBN",
    "MARC OCLC Numbers",
    "MARC LC Call Number",
    "MARC Author Personal",
    "MARC Author Corporate",
    "MARC Author Meeting",
    "MARC Title",
    "MARC Title Remainder",
    "MARC General Note",
    "MARC Subjects",
    "MARC Genres",
    # GRIN
    "Scanned Date",
    "Converted Date",
    "Downloaded Date",
    "Processed Date",
    "Analyzed Date",
    "OCR Date",
    "GRIN State",
    "GRIN Viewability",
    "GRIN Conditions",
    "GRIN Scannable",
    "GRIN Opted Out",
    "GRIN Tagging",
    "GRIN Audit",
    "GRIN Material Error %",
    "GRIN Overall Error %",
    "GRIN Claimed",
    "GRIN OCR Analysis Score",
    "GRIN OCR GTD Score",
    "GRIN Digitization Method",
]
"""
    Simplified list of fields to export.
    Focuses on basic GRIN metrics and MARC fields.
"""


@click.command("simplified-source-metadata")
@click.option(
    "--include-non-pd",
    type=bool,
    is_flag=True,
    default=False,
    help="If set, ignores rights determination checks.",
)
@utils.needs_pipeline_ready
def simplified_source_metadata(include_non_pd: bool):
    """
    Simplified CSV export of the source metadata extracted from Google Books.

    Saved as:
    - `/data/output/export/simplified-source-metadata-{pd}-{datetime}.csv`
    """
    output_filepath = None
    pd_only = not include_non_pd

    output_filepath = Path(
        EXPORT_DIR_PATH,
        f"simplified-source-metadata-{"pd-" if pd_only else ""}{DATETIME_SLUG}.csv",
    )

    ht_collection_prefix = os.getenv("HATHITRUST_COLLECTION_PREFIX", "")

    with open(output_filepath, "w+") as fd:
        writer = csv.writer(fd)

        writer.writerow(FIELDS_TO_EXPORT)

        for book in BookIO.select().order_by(BookIO.barcode).iterator():
            row = []

            if pd_only and not utils.is_pd(book):
                continue

            # Addition: Hathitrust Link
            if ht_collection_prefix:
                metadata = book.metadata

                metadata["Hathitrust Link"] = (
                    f"https://babel.hathitrust.org/cgi/pt?id={ht_collection_prefix}.{book.barcode.lower()}"
                )
            else:
                metadata["Hathitrust Link"] = ""

            for field in FIELDS_TO_EXPORT:
                row.append(book.metadata[field])

            writer.writerow(row)

    logger.info(f"{output_filepath.name} saved to disk")
