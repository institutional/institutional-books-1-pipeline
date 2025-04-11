import traceback
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import click
import iso639
import tiktoken
from pyfranc import franc
from langchain.text_splitter import RecursiveCharacterTextSplitter

import utils
from models import BookIO, MainLanguage, LanguageDetection

TOKENIZER_NAME = "o200k_base"
""" Target tokenizer to be used with tiktoken """


@click.command("run-language-detection")
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
    "--chunk-size",
    required=False,
    type=int,
    default=768,
    help="Size (in characters) of the text chunks given to franc.",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=multiprocessing.cpu_count(),
    help="Determines how many subprocesses can be run in parallel.",
)
@utils.needs_pipeline_ready
def run_language_detection(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    chunk_size: int,
    max_workers: int,
):
    """
    Runs a language detection algorithm on the OCR'd text of books, in chunks.

    For each book:
    - Collects the distribution and proportion of all identified languages in `language_detection`
    - Keeps track of the "main" detected language in `main_language` (for comparison with metadata info)

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise
    - Requires the `main_lanaguage`` table to be populated. See `analyze extract-main-language-from-metadata`
    """
    #
    # Data dependency checks
    #
    try:
        assert BookIO.select().count() == MainLanguage.select().count()
    except:
        click.echo(
            "This command needs metadata-based language information. "
            + "See `extract-main-language-from-metadata`."
        )
        exit(1)

    #
    # Process books in parallel
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
            future = executor.submit(process_book, book, chunk_size, overwrite)
            futures.append(future)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not detect languages in scanned texts. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_book(
    book: BookIO,
    chunk_size: int,
    overwrite: bool = False,
) -> bool:
    """
    Runs text-level language detection on a single books.
    - Splits text in chunks roughly identified as groups of sentences of max length X
    - Runs detection on each chunk, collects main language and token count
    - Summarize language distribution at book level
    - Update MainLanguage and LanguageDistribution tables accordingly
    """
    start_datetime = datetime.now()

    full_text = book.merged_text
    chunks = []

    total_token_count = 0
    tokens_per_language = {}

    tokenizer = None
    text_splitter = None

    # Stop here if we don't have text
    if not full_text.strip():
        click.echo(f"⏭️ #{book.barcode} does not have text.")
        return True

    # Stop here if ovewrite is `False` and we've laready processed this record
    if (
        not overwrite
        and LanguageDetection.select().where(LanguageDetection.book == book.barcode).count()
    ):
        click.echo(f"⏭️ #{book.barcode} already analyzed.")
        return True

    tokenizer = tiktoken.get_encoding(TOKENIZER_NAME)

    #
    # Split text in chunks
    #
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=0,
        length_function=len,
        is_separator_regex=False,
        separators=[
            "\n\n",
            "\n",
            " ",
            ".",
            ",",
            "\u200b",
            "\uff0c",
            "\u3001",
            "\uff0e",
            "\u3002",
            "",
        ],
    )

    chunks = text_splitter.split_text(full_text)

    #
    # For each chunk: count tokens, detect language
    #
    tokenizer = tiktoken.get_encoding(TOKENIZER_NAME)

    for i, chunk in enumerate(chunks):

        if len(chunk) < 10:
            continue

        token_count = len(tokenizer.encode(chunk))
        detection = franc.lang_detect(chunk)[0]
        lang = detection[0]

        if tokens_per_language.get(lang, None) is None:
            tokens_per_language[lang] = 0

        total_token_count += token_count
        tokens_per_language[lang] += token_count

    #
    # Sort by token count descending
    #
    tokens_per_language = dict(
        sorted(tokens_per_language.items(), key=lambda item: item[1], reverse=True)
    )

    #
    # Delete all LanguageDetection records for the current book, if any
    #
    LanguageDetection.delete().where(LanguageDetection.book == book.barcode).execute()

    #
    # Save records
    #
    entries_to_create = []

    for i, item in enumerate(tokens_per_language.items()):
        lang, token_count = item

        # Update MainLanguage record with first record (most commonly detected language)
        if i == 0 and lang != "und":
            main_language = MainLanguage.get(book=book.barcode)
            main_language.from_detection_iso693_2b = iso639.Lang(pt3=lang).pt2b
            main_language.from_detection_iso693_3 = lang
            main_language.detection_source = "pyfranc"
            main_language.save()

        # Create LanguageDetection record for this detection
        entries_to_create.append(
            LanguageDetection(
                book=book.barcode,
                iso693_2b=iso639.Lang(pt3=lang).pt2b,
                iso693_3=lang,
                token_count=token_count,
                tokenizer=TOKENIZER_NAME,
                percentage_of_total=token_count / total_token_count * 100,
                detection_source="pyfranc",
            )
        )

    # Save records
    utils.process_db_write_batch(LanguageDetection, entries_to_create, [], [])
    click.echo(f"🧮 #{book.barcode} processed in {datetime.now() - start_datetime}.")
    return True
