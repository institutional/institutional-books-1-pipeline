import traceback
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import click
import iso639
import tiktoken
from pyfranc import franc
from langchain.text_splitter import RecursiveCharacterTextSplitter
import spacy

import utils
from models import BookIO, MainLanguage, LanguageDetection

TIKTOKEN_TOKENIZER = "o200k_base"
""" Target tokenizer to be used with tiktoken """

NLP_MODEL_NAME = "xx_sent_ud_sm"
""" Name of the model to be used by spaCy (specifically: multilingual sentence splitting) """


@click.command("step02-detect")
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
def step02_detect(
    overwrite: bool,
    start: int | None,
    end: int | None,
    chunk_size: int,
    max_workers: int,
):
    """
    Language-related experiments, step 02:
    (WORK IN PROGRESS)
    """
    nlp = spacy.load(NLP_MODEL_NAME)
    nlp.max_length = 1000 * 1000 * 1000

    # Dependency: check that `main_language` was populated
    try:
        assert BookIO.select().count() == MainLanguage.select().count()
    except:
        click.echo("This command needs metadata-based language information. See step 01.")
        exit(1)

    # Process books in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():
            future = executor.submit(
                process_book,
                book,
                chunk_size,
                nlp,
                overwrite,
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not detect languages on scanned texts. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_book(
    book: BookIO,
    chunk_size: int,
    nlp: spacy.language.Language,
    overwrite: bool = False,
) -> bool:
    """ """
    start_datetime = datetime.now()

    full_text = book.merged_text
    doc = None
    sentences = []
    chunks = []

    total_token_count = 0
    tokens_per_language = {}

    tokenizer = tiktoken.get_encoding(TIKTOKEN_TOKENIZER)
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

    # Stop here if we don't have text
    if not full_text.strip():
        return True

    # Spacy model needs to be ready
    if not isinstance(nlp, spacy.language.Language):
        raise TypeError("spaCy model is not ready.")

    # Stop here if ovewrite is `False` and we've laready processed this record
    if (
        not overwrite
        and LanguageDetection.select().where(LanguageDetection.book == book.barcode).count()
    ):
        click.echo(f"⏭️ #{book.barcode} already analyzed.")
        return True

    #
    # Split by sentence (roughly)
    #
    doc = nlp(full_text)
    sentences = [sent.text for sent in doc.sents]

    # Further split sentences that are > chunk_size
    for i, sentence in enumerate(sentences):
        if len(sentence) <= chunk_size:
            continue

        split = text_splitter.split_text(sentence)
        sentences[i] = split

    #
    # Group sentences in chunks of length X
    #
    chunks.append("")  # First chunk
    chunks_i = 0  # Index of chunk to add current sentence to

    for sentence in sentences:
        chunk = chunks[chunks_i]

        # If sentence is string: add it directly to current chunk
        if isinstance(sentence, str):
            if len(chunk) + len(sentence) >= chunk_size:
                chunks.append("")
                chunks_i += 1
                chunk = chunks[chunks_i]

            chunk += f"{sentence} "
            chunks[chunks_i] = chunk
        # If sentence is a list of strings: add them separately to current chunk
        if isinstance(sentence, list):
            for sentence_chunk in sentence:
                if len(chunk) + len(sentence_chunk) >= chunk_size:
                    chunks.append("")
                    chunks_i += 1
                    chunk = chunks[chunks_i]

                chunk += f"{sentence_chunk} "
                chunks[chunks_i] = chunk

    #
    # For each chunk: count tokens, detect language
    #
    for chunk in chunks:
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
                iso639_3=lang,
                token_count=token_count,
                tokenizer=TIKTOKEN_TOKENIZER,
                percentage_of_total=token_count / total_token_count * 100,
                detection_source="pyfranc",
            )
        )

    # Save records
    utils.process_db_write_batch(LanguageDetection, entries_to_create, [], [])
    click.echo(f"🧮 {book.barcode} processed in {datetime.now() - start_datetime}.")
    return True
