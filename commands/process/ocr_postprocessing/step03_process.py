from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
import re
from pathlib import Path
import pickle
from datetime import datetime

import click
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
from const import (
    OUTPUT_MODELS_DIR_PATH,
    OUTPUT_OCR_POSTPROCESSING_DIR_PATH,
    HATHITRUST_PD_CODES,
    HATHITRUST_PD_STRING,
)

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
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
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
@utils.needs_hathitrust_rights_determination_data
@utils.needs_page_count_data
@utils.needs_language_detection_data
def step03_process(
    classifier_name: str,
    overwrite: bool,
    offset: int,
    limit: int,
    languages: list,
    max_workers: int,
):
    """
    OCR Post-processing step 03: Run post-processing on original OCR export.

    [!] Prototype

    This command:
    - Uses one of the models trained with step 02 to infer the type of each line in the original OCR export.
    - Uses the detected type and heuristics to assemble the lines into more readable text.
    - Outputs a single JSON file per book, handled via `BookIO.postprocessed_ocr`.

    Notes:
    - Whenever possible, running heads and page numbers are skipped.
    - Whenever possible chunks detected as noise will be skipped (e.g: if they're only 1 character long).
    - Only tested on the following languages: eng, deu, fra, ita, spa.
    - This is implementation is an early prototype and is therefore more effective than efficient.
    """
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
            main_language = None
            rights_determination = None

            main_language = book.mainlanguage_set[0]
            rights_determination = book.hathitrustrightsdetermination_set[0]

            assert main_language
            assert rights_determination

            if main_language.from_detection_iso639_3 not in languages:
                continue

            try:
                assert rights_determination.rights_code in HATHITRUST_PD_CODES
                assert rights_determination.us_rights_string == HATHITRUST_PD_STRING
            except:
                continue

            batch.append(book)

            # Send batch for processing
            if len(batch) >= batch_max_size or i >= items_count:
                future = executor.submit(process_batch, batch, classifier_name, overwrite)
                futures.append(future)
                batch = []

        # Process batches
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as err:
                click.echo(traceback.format_exc())
                executor.shutdown(wait=False, cancel_futures=True)
                raise err


def process_batch(books: list[BookIO], classifier_name: str, overwrite: bool = False) -> bool:
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
        output.append(process_book(book, classifier, overwrite))

    return True


def process_book(
    book: BookIO,
    classifier: StaticModelForClassification,
    overwrite: bool = False,
) -> bool:
    """
    Processes and individual book.
    """
    processing_start = datetime.now()
    processing_end = None

    output_filepath = Path(OUTPUT_OCR_POSTPROCESSING_DIR_PATH, f"{book.barcode}.json")

    text_by_page = []
    stats = {target_type: 0 for target_type in TARGET_TYPES}

    #
    # Skip if book has already been processed and overwrite mode is off
    #
    if not overwrite:
        try:
            assert book.postprocessed_ocr
            click.echo(f"⏭️ #{book.barcode} was already processed.")
            return False
        except:
            pass

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
    # Collect stats in the process
    #
    for page_chunks in book_chunks:
        # Recompose text, adjust types
        page_text = convert_page_chunks_to_text(page_chunks)
        text_by_page.append(page_text)

        # Collect stats
        for chunk in page_chunks:
            if not chunk.target_type:
                continue

            stats[chunk.target_type] += 1

    #
    # Book-level post processing
    # NOTE: WIP / Largely effective but inefficient.
    #
    remove_remaining_running_heads(text_by_page)
    remove_remaining_hyphenations(text_by_page)
    remove_remaining_page_numbers(text_by_page)

    #
    # Collect and preserve output + stats
    #
    book.postprocessed_ocr = {"stats": stats, "text_by_page": text_by_page}
    processing_end = datetime.now()
    click.echo(f"✅ {output_filepath.name} written to disk ({processing_end - processing_start})")

    return True


def convert_page_chunks_to_text(page_chunks: list[OCRPostprocessingTrainingDataset]) -> str:
    """
    Uses the detected type of OCR chunks from a given page to recompose it, focusing on readability.
    May modify the type of the chunks in place based on heuristics.
    """
    output = ""

    if not page_chunks:
        return output

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

        ## current.text = f"<<{current.target_type}>> {current.text}"

        # Skip chunks that don't contain text
        if not current.text or not current.text.strip():
            continue

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
    # Step 2 - Clean up resulting string
    #
    output = output.strip()

    # Remove double line breaks in the middle of sentences (very coarse)
    output = re.sub(r"([a-z])\n\n([a-z])", r"\1 \2", output)
    output = re.sub(r"([a-z] +)\n\n([a-z])", r"\1 \2", output)
    output = re.sub(r"(,+)\n\n([a-zA-Z0-9])", r"\1 \2", output)
    output = re.sub(r"(, +)\n\n([a-zA-Z0-9])", r"\1 \2", output)

    # Extra line breaks and missed separators
    for i in range(0, 5):
        output = output.replace("\n\n\n", "\n\n")
        output = output.replace("\n \n", "\n\n")
        output = output.replace("\n\n ", "\n\n")

        output = output.replace("-\n\n", " ")
        output = output.replace("\n\n--\n", "\n\n---\n\n")
        output = output.replace("\n\n- - \n", "\n\n---\n\n")
        output = output.replace("\n\n- -\n", "\n\n---\n\n")
        output = output.replace("\n\n-- ", "\n\n")
        output = output.replace("---\n\n---\n\n", "---\n\n")

        output = output.replace("  ", " ")

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
    hyphenation_removed = False

    # Make it a separator if it's any type of dash
    if current.text and len(current.text) <= 3 and re.match(ENDS_WITH_DASHES_REGEX, current.text):
        return "\n\n---\n\n"

    # Make it a separator if is a series of identical special characters
    if current.text and len(current.text) > 1 and re.match(r"^([^\w\s])\1*$", current.text):
        return "\n\n---\n\n"

    # Do not add to output if it's an empty-like string or a single character
    if not current.text.strip() or len(current.text) == 1:
        return ""

    # If it is made of 2 words or more, try to remove hyphenation
    if current.text and len(current.text.split(" ")) > 2:
        hyphenation_removed = remove_hyphenation_from_chunk(current, next)

    # Otherwise: add as is, followed by white space (unless hyphenation was removed)
    if hyphenation_removed:
        return current.text
    else:
        return f"{current.text} "


def process_page_number_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `PAGE_NUMBER` chunks.
    """
    # Do not add if it is likely an actual page number
    # Based on: position on page + format
    if (
        (current.line_number <= 5 or current.line_number > (current.total_lines // 10 * 9))
        and current.text
        and current.text.split(" ") == 1
    ):
        return ""

    # Otherwise: add as is, followed by white space
    current.target_type = "UNKNOWN"
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
    if current.line_number <= 10 and current.page_number >= 10:
        return ""

    # Otherwise: add as is, followed by white space
    current.target_type = "UNKNOWN"
    return f"{current.text} "


def process_heading_or_title_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `HEADING_OR_TITLE` chunks.
    """
    hyphenation_removed = remove_hyphenation_from_chunk(current, next)

    # Prepend with double line break if first of series
    if not previous or previous.target_type != "HEADING_OR_TITLE":
        current.text = f"\n\n{current.text}"

    # Add white space at the end of current text, unless a hyphenation was removed
    if not hyphenation_removed:
        current.text = f"{current.text} "

    # Append with double line break if:
    # - Last of series
    # - or -
    # - Ends with a line-breaking punctuation
    if not next or next.target_type != "HEADING_OR_TITLE":
        current.text = f"{current.text}\n\n"

    if (
        current.text
        and len(current.text) >= 2
        and re.match(LINE_BREAKING_PUNCTUATION_REGEX, current.text[-2])
    ):
        current.text = f"{current.text}\n\n"

    return current.text


def process_paragraph_chunk(
    current: OCRPostprocessingTrainingDataset,
    previous: OCRPostprocessingTrainingDataset | None = None,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> str:
    """
    String processing for `PARAGRAPH_CHUNK` chunks.
    """
    hyphenation_removed = remove_hyphenation_from_chunk(current, next)

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
        flipped = re.sub(LINE_BREAKING_PUNCTUATION_REGEX, r"\n\n\1", flipped, count=1)
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
        flipped = re.sub(LINE_BREAKING_PUNCTUATION_REGEX, r"\n\n\1", flipped, count=1)
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
    hyphenation_removed = remove_hyphenation_from_chunk(current, next)

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
    # - it did not contain a hyphenation (which we removed)
    if (
        next
        and next.text
        and next.target_type != "LOOSE_SENTENCE_OR_LIST_ITEM"
        and next.text[0].isupper()
        and not hyphenation_removed
    ):
        current.text = f"{current.text}\n\n"

    if hyphenation_removed:
        return current.text
    else:
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


def remove_hyphenation_from_chunk(
    current: OCRPostprocessingTrainingDataset,
    next: OCRPostprocessingTrainingDataset | None = None,
) -> bool:
    """
    If current chunk ends with any type of dash and next item starts with a lower case character:
    Remove hyphenation.

    Returns bool indicating whether hyphenation was removed.
    """
    hyphenation_removed = False

    # If line ends with a dash and next item starts with a lowercase character: remove hyphenation
    if (
        current.text
        and re.match(ENDS_WITH_DASHES_REGEX, current.text[-1])
        and next
        and next.text
        and next.text[0].islower()
    ):
        current.text = current.text[:-1]
        hyphenation_removed = True

    return hyphenation_removed


def remove_remaining_running_heads(text_by_page: list[str]) -> list[str]:
    """
    Attempts to detect remaining running heads in processed text.
    Modifies "text_by_page" in place
    """
    likely_running_heads_counter = {}
    likely_running_heads = set()

    #
    # Count chunks of identical text in the first few lines of each page
    # Exclude instances that:
    # - Contain less than 3 "words"
    # - Occur less than 3 times across the entire book.
    #
    for text in text_by_page:
        for i, line in enumerate(text.split("\n\n")):

            if i > 5:
                continue

            line = line.strip()

            if not line or len(line.split(" ")) < 3:
                continue

            if not likely_running_heads_counter.get(line, None):
                likely_running_heads_counter[line] = 0

            likely_running_heads_counter[line] += 1

    for key, value in likely_running_heads_counter.items():
        if value > 3:
            likely_running_heads.add(key)

    del likely_running_heads_counter

    #
    # Try to remove the identified running heads
    #
    for page_index, text in enumerate(text_by_page):

        if page_index < 10:
            continue

        for running_head in likely_running_heads:
            text = text.replace(running_head, "", 1)

        for i in range(0, 3):
            text = text.replace("\n\n \n\n", "\n\n")
            text = text.replace("\n\n\n", "\n\n")

        text_by_page[page_index] = text.strip()

    return text_by_page


def remove_remaining_hyphenations(text_by_page: list[str]) -> list[str]:
    """
    Attempts to detect and remove remaining hyphenations in processed text.
    Coarse.
    """
    for page_index, text in enumerate(text_by_page):
        text = re.sub(r"(?<![-\w])(\d+)[\-–—‒―]\s+(\w+)", r"\1\2", text)
        text_by_page[page_index] = text

    return text_by_page


def remove_remaining_page_numbers(text_by_page: list[str]) -> list[str]:
    """
    Attempts to detect and remove remaining page numbers at the top of each page.
    """
    for page_index, text in enumerate(text_by_page):
        change_made = False

        if page_index < 10:
            continue

        if re.search(r"[−–—-]\s*\d+\s*[−–—-]", text[0:10]):
            text = re.sub(r"([−–—-]\s*\d+\s*[−–—-])", "", text, count=1)
            change_made = True

        if not change_made and re.search(r"[−–—-]\s*\d+", text[0:10]):
            text = re.sub(r"([−–—-]\s*\d+)", "", text, count=1)
            change_made = True

        if not change_made and re.search(r"[0-9]+ ", text[0:10]):
            text = re.sub(r"[0-9]+ ", "", text, count=1)

        text_by_page[page_index] = text.strip()

    return text_by_page
