import click

import utils
from models import BookIO, OCRQuality


@click.command("step01-extract-from-metadata")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
)
@click.option(
    "--start",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--end",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--db-write-batch-size",
    type=int,
    required=False,
    default=10_000,
    help="Determines the frequency at which records are pushed to the database. By default: once every 10,000 record creation/update request.",
)
@utils.needs_pipeline_ready
def step01_extract_from_metadata(
    overwrite: bool,
    start: int | None,
    end: int | None,
    db_write_batch_size: int,
):
    """
    OCR quality experiments, step 01:
    Collects OCR quality metrics from each book as expressed in the collection's metadata.

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        OCRQuality.from_metadata,
        OCRQuality.metadata_source,
    ]

    for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():
        ocr_quality = None
        already_exists = False

        # Check if record already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            ocr_quality = OCRQuality.get(book=book.barcode)
            assert ocr_quality
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} already analyzed.")
                continue
        except Exception:
            pass

        # Prepare record
        ocr_quality = OCRQuality() if not already_exists else ocr_quality
        ocr_quality.book = book.barcode
        ocr_quality.metadata_source = "OCR Analysis Score"

        from_metadata = book.csv_data["OCR Analysis Score"]

        try:
            ocr_quality.from_metadata = int(from_metadata)
            assert from_metadata is not None
            ocr_quality.from_metadata = from_metadata
            click.echo(f"🧮 #{book.barcode} = {from_metadata} (metadata)")
        except:
            click.echo(f"🧮 #{book.barcode} - no valid OCR Analysis score info.")

        # Add to batch
        if already_exists:
            entries_to_update.append(ocr_quality)
        else:
            entries_to_create.append(ocr_quality)

        # Empty batches every X row
        if len(entries_to_create) + len(entries_to_update) >= db_write_batch_size:
            utils.process_db_write_batch(
                OCRQuality,
                entries_to_create,
                entries_to_update,
                fields_to_update,
            )

    # Save remaining items from batches
    utils.process_db_write_batch(
        OCRQuality,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
