from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
import random
import os
import re
from pathlib import Path
import pickle

import click
import peewee
from model2vec.inference import StaticModelPipeline
from model2vec.train import StaticModelForClassification


import utils
from models import (
    BookIO,
    OCRPostprocessingTrainingDataset,
    HathitrustRightsDetermination,
    MainLanguage,
    PageCount,
)
from models.ocr_postprocessing_training_dataset import TARGET_TYPES
from const import OUTPUT_MODELS_DIR_PATH, DATETIME_SLUG


@click.command("step03-process")
@click.option(
    "--classifier-name",
    type=str,
    required=True,
    help="Name of the Model2Vec classifier trained in step02.",
)
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
    "--pd-only",
    is_flag=True,
    default=True,
    help="If set, only processes records flagged as PD / PDUS / CC-ZERO by Hathitrust.",
)
@click.option(
    "--languages",
    type=click.Choice(["eng", "deu", "fra", "ita", "spa"]),
    multiple=True,
    required=False,
    default=["eng", "deu", "fra", "ita", "spa"],
    help="ISO 639-3 code of the languages to focus on. By default, focuses on the top 5 languages.",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=1,
    help="Determines how many threads can be run in parallel.",
)
@utils.needs_pipeline_ready
def step03_process(
    classifier_name: str,
    offset: int,
    limit: int,
    pd_only: bool,
    languages: list,
    max_workers: int,
):
    """
    TODO
    """
    #
    # Data dependencies check
    #

    # Rights determination
    if pd_only:
        try:
            assert BookIO.select().count() == HathitrustRightsDetermination.select().count()
        except:
            click.echo("Hathitrust rights determination data is not available.")
            exit(1)

    # Page count
    try:
        assert BookIO.select().count() == PageCount.select().count()
    except:
        click.echo("Page count data is not available.")
        exit(1)

    # Language detection data
    try:
        assert BookIO.select().count() == MainLanguage.select().count()

        count = (
            MainLanguage.select().where(MainLanguage.from_detection_iso639_3.is_null(False)).count()
        )
        assert count
    except:
        click.echo("This command needs language detection data.")
        exit(1)

    #
    # Check that classifier file exists
    #
    try:
        assert Path(OUTPUT_MODELS_DIR_PATH, f"{classifier_name}.pickle").exists()
    except Exception:
        click.echo(traceback.format_exc())
        click.echo(f"Fine-tuned classifier {classifier_name} does not exist. Interrupting.")
        exit(1)


def process_book(book: BookIO, classifier_name: str) -> bool:
    """
    TODO
    """
    classifier_filepath = Path(OUTPUT_MODELS_DIR_PATH, f"{classifier_name}.pickle")
    output_filepath = Path(OUTPUT_MODELS_DIR_PATH, f"{book.barcode}.json")

    classifier: StaticModelForClassification | None = None
    output = []

    #
    # Load classifier
    #
    try:
        with open(classifier_filepath, "rb+") as fd:
            classifier = pickle.load(fd)

        assert isinstance(classifier, StaticModelForClassification)
    except Exception as err:
        raise Exception(f"Could not load fine-tuned classifier {classifier_name}.") from err

    #
    # Extract bare OCR chunks for the all book
    #
    ocr_chunks = OCRPostprocessingTrainingDataset.get_chunks_from_book(book)

    #
    # Assign types to all chunks
    #
    for page_chunks in ocr_chunks:
        predictions = classifier.predict(
            [chunk.get_training_repr() for chunk in page_chunks],
            batch_size=1024 * 10,
        )

        for i, prediction in enumerate(predictions):
            page_chunks[i].target_type = TARGET_TYPES[prediction]

    #
    # Recompose the text of each page based on detected chunk types + heuristics
    #
    for page_chunks in ocr_chunks:
        page_text = convert_page_chunks_to_text(page_chunks)
        output.append(page_text)

    return output


def convert_page_chunks_to_text(page_chunks: list[OCRPostprocessingTrainingDataset]) -> str:
    """
    TODO
    """
    output = ""

    #
    # Step 1 - Add chunks to output based on type
    #
    for i in range(0, len(page_chunks)):
        current = page_chunks[i]
        previous = None
        next = None

        # Collect previous-next chunk
        if i > 0:
            previous = page_chunks[i - 1]

        if i + 1 < len(page_chunks):
            next = page_chunks[i + 1]

        # Process chunk
        if current.target_type == "UNKNOWN":
            output += process_unknown_chunk(current, previous, next)
            continue

        if current.target_type == "NOISE_OR_BROKEN_TEXT":
            output += process_noise_or_broken_text_chunk(current, previous, next)
            continue

        if current.target_type == "PAGE_NUMBER":
            output += process_page_number_chunk(current, previous, next)
            continue

        if current.target_type == "RUNNING_HEAD":
            output += process_running_head_chunk(current, previous, next)
            continue

        if current.target_type == "HEADING_OR_TITLE":
            output += process_heading_or_title_chunk(current, previous, next)
            continue

        if current.target_type == "PARAGRAPH_CHUNK":
            output += process_paragraph_chunk(current, previous, next)
            continue

        if current.target_type == "PARAGRAPH_END":
            output += process_paragraph_end_chunk(current, previous, next)
            continue

        if current.target_type == "LOOSE_SENTENCE_OR_LIST_ITEM":
            output += process_loose_sentence_or_list_item_chunk(current, previous, next)
            continue

        if current.target_type == "SEPARATOR":
            output += process_separator_chunk(current, previous, next)
            continue

    #
    # Step 2 - Process resulting string
    #
    for i in range(0, 5):
        output = output.replace("\n\n\n", "\n\n")

    return output


def process_unknown_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    return f"{current.text} "


def process_noise_or_broken_text_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    # TODO
    return ""


def process_page_number_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    last_10percent_lines = current.total_lines // 10 * 9

    # Do not render if they are likely to actually be page numbers
    if current.line_number < 5 or current.line_number > last_10percent_lines:
        return ""

    return f"{current.text} "


def process_running_head_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    # Do not render if likely running head
    if current.line_number < 10 and current.page_number > 5:
        return ""

    return f"{current.text} "


def process_heading_or_title_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    # TODO
    # Figure out edge cases towards the end
    # Further break down headings if they contain an hyphenation?
    # If next line is not PARAGRAPH_X or LOOSE_SENTENCE_OR_LIST_ITEM and it doesn't start with either a capital letter or a number: don't render

    output = ""

    # Don't render if next line is a page number
    if next and next.target_type == "PAGE_NUMBER":
        return output

    # Don't render if next line:
    # - is a PARAGRAPH_X or LOOSE_SENTENCE_OR_LIST_ITEM
    # - that starts with a lower case character
    if (
        next
        and next.target_type
        not in ["PARAGRAPH_CHUNK", "PARAGRAPH_END", "LOOSE_SENTENCE_OR_LIST_ITEM"]
        and next.text
        and next.text[0].islower()
    ):
        return output

    # Add markdown heading marker if first of series
    if previous and previous.target_type != "HEADING_OR_TITLE":
        output += "\n\n## "

    output += f"{current.text} "

    ## Add double line break if last of series
    if next and next.target_type != "HEADING_OR_TITLE":
        output += "\n\n"

    return output


def process_paragraph_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    # TODO
    # Remove hyphenation
    # Add double line-break is line contains a punctuation in the last third
    return f"{current.text} "


def process_paragraph_end_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    # TODO
    output = ""

    if [".", "!", ";", ":", "?"] in list(current.text):
        output += f"{current.text}\n\n"
    else:
        output += f"{current.text} "

    return output


def process_loose_sentence_or_list_item_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    # TODO
    output = ""

    if [".", "!", ";", ":", "?"] in list(current.text):
        output += f"{current.text}\n\n"
    else:
        output += f"{current.text} "

    return output


def process_separator_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    # TODO
    return "\n\n---\n\n"
