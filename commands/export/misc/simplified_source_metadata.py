from pathlib import Path
import csv

import click

import utils
from const import OUTPUT_EXPORT_DIR_PATH, DATETIME_SLUG
from models import BookIO, HathitrustRightsDetermination

FIELDS_TO_EXPORT = [
    "Barcode",
    "Google Books Link",
    # Only addition: Hathitrust link
    "Hathitrust Link",
    # GXML
    "gxml Control Number",
    "gxml Date 1",
    "gxml Date 2",
    "gxml Date Type",
    "gxml Language",
    "gxml Library of Congress Control Number",
    "gxml ISBN",
    "gxml OCoLC Number(s)",
    "gxml Library of Congress Call Number",
    "gxml Author (Personal Name)",
    "gxml Author (Corporate Name)",
    "gxml Author (Meeting Name)",
    "gxml Title",
    "gxml Title Remainder",
    "gxml General Note",
    "gxml Subject Added Entry-Topical Term",
    "gxml Index Term-Genre/Form",
    # GRIN
    "Scanned Date",
    "Converted Date",
    "Downloaded Date",
    "Processed Date",
    "Analyzed Date",
    "OCR'd Date",
    "Page Count",
    "State",
    "Viewability",
    "Condition-at-checkin Code",
    "Scannable",
    "Opted-Out (post-scan)",
    "Tagged",
    "Audit",
    "Material Error %",
    "Overall Error %",
    "Claimed",
    "OCR Analysis Score",
    "OCR Garbage Detection Score",
    "Digitization Method",
]
"""
    Simplified list of fields to export.
    Focuses on basic GRIN metrics and gxml fields.
    Source: https://github.com/instdin/grin-to-s3-internal/blob/main/Data%20Dictionary.md
"""


@click.command("simplified-source-metadata")
@click.option(
    "--pd-only",
    is_flag=True,
    default=False,
    help="If set, only exports records flagged as PD / PDUS / CC-ZERO by Hathitrust.",
)
@utils.needs_pipeline_ready
def simplified_source_metadata(pd_only: bool):
    """
    Simplified CSV export of the metadata originally coming from GRIN.

    Saved as:
    - `/data/output/export/simplified-source-metadata-{pd}-{datetime}.csv`
    """
    output_filepath = None

    # Dependencies check
    if pd_only:
        try:
            assert BookIO.select().count() == HathitrustRightsDetermination.select().count()
        except:
            click.echo("Hathitrust rights determination data is not available.")
            exit(1)

    output_filepath = Path(
        OUTPUT_EXPORT_DIR_PATH,
        f"simplified-source-metadata-{"pd-" if pd_only else ""}{DATETIME_SLUG}.csv",
    )

    with open(output_filepath, "w+") as fd:
        writer = csv.writer(fd)

        writer.writerow(FIELDS_TO_EXPORT)

        for book in BookIO.select().order_by(BookIO.barcode).iterator():
            row = []

            if pd_only:
                try:
                    rights_determination = book.hathitrustrightsdetermination_set[0]
                    assert rights_determination.rights_code in ["pd", "pdus", "cc-zero"]
                    assert rights_determination.us_rights_string == "Full view"
                except:
                    continue

            # Addition: Hathitrust Link
            csv_data = book.csv_data

            csv_data["Hathitrust Link"] = (
                f"https://babel.hathitrust.org/cgi/pt?id=hvd.{book.barcode.lower()}"
            )

            for field in FIELDS_TO_EXPORT:
                row.append(book.csv_data[field])

            writer.writerow(row)

    click.echo(f"✅ {output_filepath.name} saved to disk.")
