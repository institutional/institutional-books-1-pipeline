from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
import re
from pathlib import Path
import pickle
from datetime import datetime
import json

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
from const import OUTPUT_MODELS_DIR_PATH, OUTPUT_OCR_POSTPROCESSING_DIR_PATH, DATETIME_SLUG

LINE_BREAKING_PUNCTUATION_REGEX = r"([.!;:?])"
""" Regex focusing on characters that can be considered "line-breaking" in certain contexts. """

ENDS_WITH_DASHES_REGEX = r"[-‐‑‒–—―−⸺⸻﹘﹣－]$"
""" Regex focusing on strings ending with any type of dash. """


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

    #
    # Create batches of books, process them in parallel
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        batch = []

        items_count = BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).count()

        batch_max_size = utils.get_batch_max_size(
            items_count=items_count,
            max_workers=max_workers,
        )

        # Create batches of items to process
        for i, book in enumerate(
            BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator(),
            start=1,
        ):

            # Add book if it matches criteria (language selection, PD)
            main_language = book.mainlanguage_set[0]
            rights_determination = book.hathitrustrightsdetermination_set[0]

            if main_language.from_detection_iso639_3 not in languages:
                continue

            if (
                rights_determination.rights_code not in ["pd", "pdus", "cc-zero"]
                or rights_determination.us_rights_string != "Full view"
            ):
                continue

            batch.append(book)

            # Send batch for processing
            if len(batch) >= batch_max_size or i >= items_count:
                future = executor.submit(process_batch, batch, classifier_name)
                futures.append(future)
                batch = []

        # Process batches
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as err:
                executor.shutdown(wait=False, cancel_futures=True)
                raise err


def process_batch(books: list[BookIO], classifier_name: str) -> bool:
    """
    Processes a batch of books.
    """
    classifier_filepath = Path(OUTPUT_MODELS_DIR_PATH, f"{classifier_name}.pickle")
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

    for book in books:
        output.append(process_book(book, classifier))

    return True


def process_book(book: BookIO, classifier: StaticModelForClassification) -> bool:
    """
    Re-processes the plain text OCR export of all the pages in a given book.
    Saved output as
    """
    processing_start = datetime.now()
    processing_end = None

    output_filepath = Path(OUTPUT_OCR_POSTPROCESSING_DIR_PATH, f"{book.barcode}.json")

    output = []

    #
    # Extract bare OCR chunks for the all book
    #
    book_chunks = OCRPostprocessingTrainingDataset.get_chunks_from_book(book)

    #
    # Assign types to all chunks
    #
    for page_chunks in book_chunks:
        predictions = classifier.predict(
            [chunk.get_training_repr() for chunk in page_chunks],
            batch_size=1024 * 10,
        )

        for i, prediction in enumerate(predictions):
            page_chunks[i].target_type = TARGET_TYPES[prediction]

    #
    # Recompose the text of each page based on detected chunk types + heuristics
    #
    for page_chunks in book_chunks:
        page_text = convert_page_chunks_to_text(page_chunks)
        output.append(page_text)

    #
    # Write output to disk
    #
    with open(output_filepath, "w+") as fd:
        json.dump(output, fd)

    processing_end = datetime.now()
    click.echo(f"✅ {output_filepath.name} written to disk ({processing_end - processing_start})")

    return output


def convert_page_chunks_to_text(page_chunks: list[OCRPostprocessingTrainingDataset]) -> str:
    """
    Uses the detected type of OCR chunks from a given page to recompose it, focusing on readability.
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

        # Skip chunks that don't contain text
        if not current.text or not current.text.strip():
            continue

        # output += f"<<{current.target_type}>>"

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
    # Step 2 - Clean up resulting string from excess line breaks
    #
    for i in range(0, 10):
        output = output.replace("\n\n\n", "\n\n")
        output = output.replace("\n \n", "\n\n")

    output = output.strip()

    return output


def process_unknown_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `UNKNOWN` chunks.
    """
    # Add as is, followed by white space
    return f"{current.text} "


def process_noise_or_broken_text_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `NOISE_OR_BROKEN_TEXT` chunks.
    """
    # Make it a separator if it's any type of dash
    if current.text and len(current.text) <= 3 and re.match(ENDS_WITH_DASHES_REGEX, current.text):
        return "\n\n---\n\n"

    # Do not add to output if it is a single character or empty-like string
    if not current.text.strip() or re.match(r"^[^\w\s]+$", current.text):
        return ""

    # Otherwise: add as is, followed by white space
    return f"{current.text} "


def process_page_number_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `PAGE_NUMBER` chunks.
    """
    # Do not add if it is likely an actual page number (based on position in page)
    if current.line_number <= 5 or current.line_number > (current.total_lines // 10 * 9):
        return ""

    # Otherwise: add as is, followed by white space
    return f"{current.text} "


def process_running_head_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `RUNNING_HEAD` chunks.
    """
    # Do not add if it is likely an actual RUNNING HEAD chunk (based on position in page and in book)
    if current.line_number <= 10 and current.page_number >= 5:
        return ""

    # Otherwise: add as is, followed by white space
    return f"{current.text} "


def process_heading_or_title_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `HEADING_OR_TITLE` chunks.
    """
    output = ""

    # Skip if more likely to be a RUNNING_HEAD chunk, based on:
    # - Position in page (first five lines)
    # - Type of next chunk (e.g: followed by a PAGE_NUMBER)
    # - Content of next chunk (e.g: starts with a lower case character)
    if current.line_number <= 5 and next and next.target_type == "PAGE_NUMBER":
        return output

    if current.line_number <= 5 and next and next.text and next.text[0].islower():
        return output

    # Prepend with double line break if first of series
    if not previous or previous.target_type != "HEADING_OR_TITLE":
        output += "\n\n"

    # Add actual text followed by a white space, in all cases
    output += f"{current.text} "

    # Append with double line break if last of series
    if not next or next.target_type != "HEADING_OR_TITLE":
        output += "\n\n"

    return output


def process_paragraph_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `PARAGRAPH_CHUNK` chunks.
    """
    hyphenation_removed = False

    # Try to remove hyphenation
    if current.text and re.match(ENDS_WITH_DASHES_REGEX, current.text[-1]):
        current.text = current.text[:-1]
        hyphenation_removed = True

    # Inject double line breaks around the last line-breaking punctuation of the line if:
    # - The line contains more than 1 "word"
    # - The chunk is preceded and followed by a `PARAGRAPH_CHUNK` or `LOOSE_SENTENCE_OR_LIST_ITEM`
    # - The line contains a line-breaking punctuation towards the end
    seq_types = ["PARAGRAPH_CHUNK", "LOOSE_SENTENCE_OR_LIST_ITEM"]

    if (
        current.text
        and len(current.text.strip().split(" ")) > 1
        and re.search(LINE_BREAKING_PUNCTUATION_REGEX, current.text)
        and (not previous or previous.target_type not in seq_types)
        and (not next or next.target_type not in seq_types)
    ):
        flipped = current.text[::-1]
        flipped = re.sub(LINE_BREAKING_PUNCTUATION_REGEX, r"\1\n\n", flipped, count=1)
        current.text = flipped[::-1]

    #
    # Inject double line break before line if:
    # - Last line ends with a punctuation
    # - This line starts with a number or a uppercase character
    #
    if (
        previous
        and previous.text
        and re.search(LINE_BREAKING_PUNCTUATION_REGEX, previous.text)
        and current.text
        and (current.text[0].isdigit() or current.text[0].isupper())
    ):
        current.text = f"\n\n{current.text}"

    # Do not add whitespace if a hyphenation was removed
    if hyphenation_removed:
        return current.text
    else:
        return f"{current.text} "


def process_paragraph_end_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `PARAGRAPH_END` chunks.
    """
    # Inject double line breaks around the last line-breaking punctuation of the line if:
    # - The line contains more than 1 "word"
    # - The line contains a line-breaking punctuation
    if (
        current.text
        and len(current.text.strip().split(" ")) > 1
        and re.search(LINE_BREAKING_PUNCTUATION_REGEX, current.text)
    ):
        flipped = current.text[::-1]
        flipped = re.sub(LINE_BREAKING_PUNCTUATION_REGEX, r"\1\n\n", flipped, count=1)
        current.text = flipped[::-1]

    return f"{current.text}\n\n"


def process_loose_sentence_or_list_item_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `LOOSE_SENTENCE_OR_LIST_ITEM` chunks.
    """

    # If it contains more than 1 word and starts with a number or upper case character:
    # Prepend it with two line breaks
    if (
        current.text
        and len(current.text.split(" ")) > 1
        and (current.text[0].isdigit() or current.text[0].isupper())
    ):
        current.text = f"\n\n{current.text}"

    # Add double line break if chunk is:
    # - not followed by another LOOSE_SENTENCE_OR_LIST_ITEM
    # - not followed by a line that starts with a lowercase character
    if (
        next
        and next.text
        and next.target_type != "LOOSE_SENTENCE_OR_LIST_ITEM"
        and next.text[0].isupper()
    ):
        current.text = f"{current.text}\n\n"

    return f"{current.text} "


def process_separator_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `SEPARATOR` chunks.
    """
    # Skip empty separators
    if not current.text:
        return ""

    return "\n\n---\n\n"
