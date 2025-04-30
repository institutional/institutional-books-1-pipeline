from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import random
import os
import re

import click
import peewee
import ollama


import utils
from models import (
    BookIO,
    OCRPostprocessingTrainingDataset,
    HathitrustRightsDetermination,
    MainLanguage,
    TextAnalysis,
    PageCount,
)
from models.ocr_postprocessing_training_dataset import TARGET_TYPES

TARGET_MODEL = "phi4:14b-q8_0"
""" Model used for generating classification data. """

TRAINING_SET_GENERATION_SYSTEM_PROMPT = f"""
You are a text classifier, helping with the post-processing of OCR text extracted from books.

You will be given one or multiple text chunks to analyze as well as some contextual information.
In this experiment, 1 chunk = 1 line extracted from a plain text OCR export. 

## Information will be structured as follows:
- `<context`: Information about the text excerpt, such as: the page number of the book this excerpt is from, the position of this chunk on the page, and the book's main language.
- `<current>` The text chunk to analyze.
- `<previous>` The text chunk that precedes the one to analyze, if any.
- `<next>` The text chunk that follows the one to analyze, if any.

## Your role is:
- To determine the TYPE of the text chunk in `<current>`. You should use all of the information available to help make that determination, not just the text in `<current>`. Carefully analyze all of the information you are given.
- To return that TYPE, and nothing else. Your response MUST be one of the TYPES listed, it cannot be anything else.

## Possible values for TYPE:
- {"\n- ".join(TARGET_TYPES)}

Carefully analyse the information you are given to accurately determine the type of the text in `<current>`. Return a TYPE and nothing else.
"""


@click.command("step01-generate-training-dataset")
@click.option(
    "--n-samples",
    type=int,
    required=False,
    default=100,
    help="Number of books/pages to pull.",
)
@click.option(
    "--pd-only",
    is_flag=True,
    default=True,
    help="If set, only exports records flagged as PD / PDUS / CC-ZERO by Hathitrust.",
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
def step01_generate_training_dataset(
    n_samples: int,
    pd_only: bool,
    languages: list,
    max_workers: int,
):
    """
    OCR Post-processing step01:
    Generating a training dataset.

    This command uses at text-generation model to label line-level OCR chunks.
    This data can then be used to be used to train a (coarse) classification model.

    TODO: Multi GPU support.
    """
    books = []
    books_chunks = {}

    train_cap = 0
    train_count = 0
    test_cap = 0
    test_count = 0

    if n_samples < 10:
        click.echo("Cannot generate a set with less than 10 samples.")
        exit(1)

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

    # Text analysis
    try:
        assert BookIO.select().count() == TextAnalysis.select().count()
    except:
        click.echo("Text analysis data is not available.")
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
    # Delete existing set
    #
    if OCRPostprocessingTrainingDataset.select().count():
        OCRPostprocessingTrainingDataset.delete().execute()

    #
    # Define max number of books per set
    #
    train_cap = round(90 * n_samples / 100)
    train_count = 0

    test_cap = round(10 * n_samples / 100)
    test_count = 0

    if train_cap + test_cap < n_samples:
        train_cap += n_samples - (train_cap + test_cap)

    #
    # Pick `n-samples` books where:
    # - `MainLanguage.from_detection_iso639_3`` matches selected language(s)
    # - `TextAnalysis.word_count`` > 1000
    # - Rights determination indicates the book is in the public domain
    #
    for book in BookIO.select().order_by(peewee.fn.Random()).iterator():
        if len(books) >= n_samples:
            break

        main_language = book.mainlanguage_set[0]
        text_analysis = book.textanalysis_set[0]
        rights_determination = book.hathitrustrightsdetermination_set[0]

        if main_language.from_detection_iso639_3 not in languages:
            continue

        if text_analysis.word_count <= 1000:
            continue

        if (
            rights_determination.rights_code not in ["pd", "pdus", "cc-zero"]
            or rights_determination.us_rights_string != "Full view"
        ):
            continue

        books.append(book)

    random.shuffle(books)

    #
    # For each book:
    # - Pick a random page and get OCR chunks from it
    # - Assign them a to set (train/test)
    #
    for book in books:
        total_pages = book.pagecount_set[0].count_from_ocr
        page = random.randint(0, total_pages - 1)

        # Get OCR chunks
        chunks = OCRPostprocessingTrainingDataset.get_chunks_from_page(book, page)

        # Determine which set these chunks belong to, based on respective caps
        set = None

        if train_count < train_cap:
            set = "train"
            train_count += 1

        if not set and test_count < test_cap:
            set = "test"
            test_count += 1

        for chunk in chunks:
            chunk.set = set

        books_chunks[book.barcode] = chunks

    #
    # Process batches of chunks in parallel, save batches as they come
    #
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for barcode, chunks in books_chunks.items():
            future = executor.submit(process_page_chunks, chunks)
            futures[future] = barcode

        # Save records as they come back
        for future in as_completed(futures):
            barcode = futures[future]

            try:
                chunks = future.result()
                books_chunks[barcode] = chunks

                for chunk in chunks:
                    click.echo(f"#{barcode} | {chunk.get_training_repr()} -> {chunk.target_type}")

                utils.process_db_write_batch(
                    OCRPostprocessingTrainingDataset,
                    chunks,
                    [],
                    [],
                )

            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not generate OCR postprocessing training set. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)

    click.echo("✅ OCR postprocessing training set ready.")


def process_page_chunks(chunks: list[OCRPostprocessingTrainingDataset]):
    """
    Annotates all of the OCR chunks from a given page.
    """
    current: OCRPostprocessingTrainingDataset | None = None
    previous: OCRPostprocessingTrainingDataset | None = None
    next: OCRPostprocessingTrainingDataset | None = None

    for i in range(0, len(chunks)):
        current = chunks[i]

        # Grab previous and next chunk, if available
        if i - 1 > 0:
            previous = chunks[i - 1]

        if i + 1 < len(chunks):
            next = chunks[i + 1]
        else:
            next = None

        # Clear previous / next if barcode doesn't match
        if previous and previous.book != current.book:
            previous = None

        if next and next.book != current.book:
            next = None

        # Attempt labelling, skip if there was an issue
        try:
            assign_ocr_chunk_type(current, previous, next)
        except Exception:
            click.echo(traceback.format_exc())
            click.echo(
                f"Couldn't generate training data for chunk {current.get_training_repr()}. Skipping."
            )

    return chunks


def assign_ocr_chunk_type(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> OCRPostprocessingTrainingDataset:
    """
    Uses a text generation model to assign one of TARGET_TYPES to  the current OCR chunk.
    """
    #
    # Skip LLM processing for simple cases
    #

    # Empty lines
    if not current.text or re.match(r"^[^a-zA-Z0-9\s]$", current.text):
        current.target_type = "NOISE_OR_BROKEN_TEXT"
        return current

    # Likely page numbers in first few lines
    if current.line_number < 4 and re.match(
        r"^[^a-zA-Z0-9]*[+-]?(\d+(\.\d+)?|\.\d+)[^a-zA-Z0-9]*$",
        current.text,
    ):
        current.target_type = "PAGE_NUMBER"
        return current

    #
    # Process anything else with an LLM
    #
    prompt = TRAINING_SET_GENERATION_SYSTEM_PROMPT
    ollama_client = ollama.Client(host=os.getenv("OLLAMA_HOST", None))
    target_type = ""

    # Remove RUNNING_HEAD from prompt if:
    # - In the first 5 pages
    # - In the last 5% of the book
    # - We're past line 5
    if (
        current.page_number < 5
        or current.page_number > round(current.total_pages // 20 * 19)
        or current.line_number > 5
    ):
        prompt = prompt.replace("- RUNNING_HEAD\n", "")

    # Remove PAGE_NUMBER unless prompt if we're either:
    # - In the first 5 lines
    # - In the last 20% of lines
    if current.line_number > 5 and current.line_number < round(current.total_lines // 4 * 3):
        prompt = prompt.replace("- PAGE_NUMBER\n", "")

    # Run completion
    response = ollama_client.ChatResponse = ollama_client.chat(
        model=TARGET_MODEL,
        messages=[
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": get_auto_annotation_repr(current, previous, next),
            },
        ],
        options={
            "temperature": 0,
            "num_predict": 25,
        },
    )

    # Grab target type, check if its valid
    target_type = response["message"]["content"]

    match = re.search(r"\b(?:[A-Z]+_?)+\b", target_type)

    if match:
        target_type = match.group()

    assert target_type in TARGET_TYPES

    # Update OCR and return OCR Chunk
    current.target_type = target_type

    return current


def get_auto_annotation_repr(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    Returns a contextualized representation for a given OCR chunk.
    This representation can be used to generate training data with an text generation model.

    Example:
    ```
    <context>Page 12 of 320, Line 4 of 128</context>
    <previous>Lorem ipsum </previous>
    <current>dolor sit</current>
    <next>amet.</next>
    ```
    """
    output = ""

    # Context
    output += f"<context>"
    output += f"Page {current.page_number+1} of {current.total_pages}, "
    output += f"Line {current.line_number+1} of {current.total_lines}, "
    output += f"Language: {current.book.mainlanguage_set[0].from_detection_iso639_3}"
    output += f"</context>\n"

    # Previous chunk
    if previous:
        output += f"<previous>{previous.text}</previous>\n"

    # Current chunk
    output += f"<current>{current.text}</current>\n"

    # Next chunk
    if next:
        output += f"<next>{next.text}</next>"

    return output
