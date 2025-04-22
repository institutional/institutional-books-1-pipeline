# TODO: Selective processing
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
from datetime import datetime

import click
from transformers import LayoutLMv3ForTokenClassification
from tmp.layoutreader.v3.helpers import prepare_inputs, boxes2inputs, parse_logits


import utils
from models import BookIO, LayoutData, PageCount
from models.layout_data import (
    PageMetadata,
    OCRChunk,
    OCRChunkType,
    LayoutSeparator,
    LayoutSeparatorType,
)


@click.command("step04-sort-spans")
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
    "--device",
    type=str,
    required=False,
    default="cpu",
    help=f"If set, allows to specify on which device the model should run ({",".join(utils.get_torch_devices())}).",
)
@utils.needs_pipeline_ready
def step04_sort_spans(
    offset: int | None,
    limit: int | None,
    device: str | None,
):
    """
    Layout-aware text processing, step 04:
    Sort spans previously assembled in reading order using hantian/layoutreader

    Notes:
    - This step doesn't automatically parallelize, because it runs a small model on GPU.
    - Updates `LayoutData` files in place.
    """
    #
    # Data dependency checks
    #
    try:
        assert BookIO.select().count() == PageCount.select().count()
    except:
        click.echo("This command needs page count information.")
        exit(1)

    #
    # Load model
    #
    model = LayoutLMv3ForTokenClassification.from_pretrained("hantian/layoutreader").to(device)

    #
    # Process books in range
    #
    for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
        # Check if there is a layout data file available for this book
        if not LayoutData.exists(barcode=book.barcode):
            click.echo(f"⏭️ #{book.barcode} does not have layout data available. Skipping.")
            continue

        processing_start = datetime.now()
        processing_end = None
        total_sorted_spans = 0

        page_count = book.pagecount_set[0].count_from_ocr
        layout_data = LayoutData.get(barcode=book.barcode)

        # We need to have pre-processed spans
        if layout_data.words_by_page is None or layout_data.separators_by_page is None:
            click.echo(f"⏭️ #{book.barcode} is missing spans data from step03. Skipping.")
            continue

        #
        # Sort spans for each page
        #
        for page in range(0, page_count):
            page_metadata = layout_data.page_metadata_by_page[page]
            spans_unsorted = layout_data.spans_by_page[page]

            normalized_bboxes_unsorted: list[list] = []
            spans_sorted: list[OCRChunk] = []

            # Scale/normalize bboxes to LayoutReader's expected range
            # See: https://github.com/ppaanngggg/layoutreader/
            x_scale = 1000.0 / page_metadata.width
            y_scale = 1000.0 / page_metadata.height

            for span in spans_unsorted:
                # Scale
                left = round(span.x_min * x_scale)
                right = round(span.y_min * y_scale)
                top = round(span.x_max * x_scale)
                bottom = round(span.y_max * y_scale)

                # Clip
                left = max(0, min(left, 1000))
                right = max(left, min(right, 1000))
                top = max(0, min(top, 1000))
                bottom = max(top, min(bottom, 1000))

                normalized_bboxes_unsorted.append([left, right, top, bottom])

            # Predict reading order
            inputs = boxes2inputs(normalized_bboxes_unsorted)
            inputs = prepare_inputs(inputs, model)
            logits = model(**inputs).logits.squeeze(0)

            # Sort spans based on reading order
            for index in parse_logits(logits, len(normalized_bboxes_unsorted)):
                spans_sorted.append(spans_unsorted[index])

            layout_data.spans_by_page[page] = spans_sorted
            total_sorted_spans += len(spans_sorted)

        #
        # Save sorted spans for that book
        #
        layout_data.spans_sorted = True
        layout_data.save()

        processing_end = datetime.now()

        click.echo(
            f"⏱️ #{book.barcode} - {total_sorted_spans} spans sorted. "
            + f"Processed in {processing_end - processing_start}"
        )
