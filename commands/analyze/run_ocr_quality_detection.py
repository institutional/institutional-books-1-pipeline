import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback

import click
from ocroscope import ocr_evaluation

import utils
from models import BookIO, OCRQuality


@click.command("run-ocr-quality-detection")
@click.option(
    "--offset",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--limit",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=multiprocessing.cpu_count(),
    help="Determines how many subprocesses can be run in parallel.",
)
@utils.needs_pipeline_ready
def run_ocr_quality_detection(
    offset: int | None,
    limit: int | None,
    max_workers: int,
):
    """
    Runs Pleais/OCROScope on the OCR'd text of each book as a way to collect a secondary OCR quality metric.

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise
    - Requires the `ocr_quality` table to be populated, see `analyze extract-ocr-quality-from-metadata` (for comparison with metadata info)
    """
    #
    # Dependency: check that `ocr_quality` was populated with metadata
    #
    try:
        assert BookIO.select().count() == OCRQuality.select().count()
    except:
        click.echo(
            "This command needs metadata-based OCR quality data. "
            + "See `extract-ocr-quality-from-metadata`."
        )
        exit(1)

    #
    # Process batches of books in parallel
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        batch = []

        items_count = OCRQuality.select().offset(offset).limit(limit).count()

        batch_max_size = utils.get_batch_max_size(
            items_count=items_count,
            max_workers=max_workers,
        )

        # Create batches of items to process
        for i, ocr_quality in enumerate(
            OCRQuality.select().offset(offset).limit(limit).order_by(OCRQuality.book).iterator(),
            start=1,
        ):
            batch.append(ocr_quality)

            if len(batch) >= batch_max_size or i >= items_count:
                future = executor.submit(process_batch, batch)
                futures.append(future)
                batch = []

        # Run them in parallel processes, update records as they come back
        for future in as_completed(futures):
            try:
                items = future.result()

                utils.process_db_write_batch(
                    OCRQuality,
                    [],
                    items,
                    [OCRQuality.from_detection, OCRQuality.detection_source],
                )

            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not detect OCR quality in scanned texts. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_batch(items: list[OCRQuality]) -> list[OCRQuality]:
    """
    Processes a batch of OCRQuality entries.
    Runs OCROScope on the text of each book associated with an OCRQuality record.
    """
    for ocr_quality in items:
        text = ocr_quality.book.merged_text

        if not text.strip():
            click.echo(f"⏭️ #{ocr_quality.book.barcode} does not have text.")
            continue

        try:
            analysis = ocr_evaluation(text=text)
            analysis.calculate_ocr_rate()
            ocr_quality.from_detection = int(analysis.ratio_segment)
            ocr_quality.detection_source = "pleias/OCRoscope"
            click.echo(f"🧮 #{ocr_quality.book.barcode} = {ocr_quality.from_detection}")
        except:
            click.echo(f"⏭️ #{ocr_quality.book.barcode} could not be analyzed.")

    return items
