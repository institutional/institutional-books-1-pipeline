import traceback
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import Counter
from datetime import datetime

import click
import iso639
import polyglot
import polyglot.text
import tiktoken

import utils
from utils import get_batch_max_size, process_db_write_batch, get_db
from models import BookIO, TextAnalysis, OCRPostProcessingTextAnalysis

TOKENIZER_NAME = "o200k_base"
""" Target tokenizer to be used with tiktoken """


@click.command("run-text-analysis")
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
    "--max-workers",
    type=int,
    required=False,
    default=multiprocessing.cpu_count(),
    help="Determines how many subprocesses can be run in parallel.",
)
@click.option(
    "--use-postprocessed-ocr",
    is_flag=True,
    default=False,
    help="If set, will run the analysis on OCR-postprocessed texts.",
)
@utils.needs_pipeline_ready
@utils.needs_language_detection_data
def run_text_analysis(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    max_workers: int,
    use_postprocessed_ocr: bool,
):
    """
    Runs simple text analysis methods on the OCR'd text of each entry in the collection.

    Collects metrics such as:
    - character/word/bigram/trigram/sentence counts.
    - token-type ratios.
    - tokenizability (how "well" a given text tokenizes using `o200k_base`).

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise
    """
    #
    # Process books in batches
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        items_count = BookIO.select().offset(offset).limit(limit).count()

        batch_max_size = get_batch_max_size(
            items_count=items_count,
            max_workers=max_workers,
        )

        books_buffer = []
        """ Single series of book of length batch_max_size """

        #
        # Create batches of books to process
        #
        for i, book in enumerate(
            BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator(),
            start=1,
        ):
            books_buffer.append(book)

            # Run if buffer is full or we've reached last item
            if len(books_buffer) >= batch_max_size or i >= items_count:
                batch = executor.submit(
                    process_books_batch, books_buffer, overwrite, use_postprocessed_ocr
                )
                futures.append(batch)
                books_buffer = []

        #
        # Analyze batches in parallel
        #
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not run text analysis on OCR'd texts. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_books_batch(
    books: list[BookIO],
    overwrite: bool = False,
    use_postprocessed_ocr: bool = False,
) -> tuple:
    """
    Generates text analysis metrics for a set of books and saves them.
    """
    tokenizer = tiktoken.get_encoding(TOKENIZER_NAME)
    entries_to_create = []
    entries_to_update = []

    model = TextAnalysis if not use_postprocessed_ocr else OCRPostProcessingTextAnalysis

    for book in books:
        start_datetime = datetime.now()
        text_analysis = None
        already_exists = False
        merged_text = None

        #
        # Check if record already exists
        #
        try:
            text_analysis = model.get(book=book.barcode)  # throws if not found
            assert text_analysis
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} already analyzed.")
                continue
        except Exception:
            pass

        #
        # Prepare record, analyze merged text
        #
        text_analysis = model() if not already_exists else text_analysis

        text_analysis.book = book.barcode
        text_analysis.char_count = 0
        text_analysis.char_count_continous = 0

        try:
            if not use_postprocessed_ocr:
                merged_text = book.merged_text
            else:
                merged_text = "\n".join(book.postprocessed_ocr["text_by_page"])
        except:
            merged_text = ""

        # We will create an empty record if no text is available.
        if not merged_text.strip():
            click.echo(f"⏭️ #{book.barcode} does not have text.")
        else:
            #
            # Split text into words/sentences/n-grams
            #

            # Get language code hint
            language_code = None

            # ... from detection if available
            try:
                language_code = book.mainlanguage_set[0].from_detection_iso639_3
                language_code = iso639.Lang(pt3=language_code).pt1
                assert language_code
            except:
                language_code = None

            # ... from metadata otherwise, default to "en" if none is available
            try:
                assert language_code is None
                language_code = book.mainlanguage_set[0].from_metadata_iso639_2b
                language_code = iso639.Lang(pt2b=language_code).pt1
                assert language_code
            except:
                language_code = "en"

            # NOTE: The decision to remove \u200b in that context was made after initial rounds of testing.
            # This decision should be revisited.
            nlp_text = polyglot.text.Text(
                merged_text.replace("\u200b", "").replace("\n", ""),
                hint_language_code=language_code,
            )

            words = [str(word).lower() for word in nlp_text.words]
            sentences = [str(sentence).lower() for sentence in nlp_text.sentences]
            bigrams = [" ".join(words[i : i + 2]).lower() for i in range(0, len(words) - 1)]
            trigrams = [" ".join(words[i : i + 3]).lower() for i in range(0, len(words) - 2)]

            #
            # Compute: fragment counts (total/unique) and type token ratios
            #
            words_counter = Counter(words)
            bigrams_counter = Counter(bigrams)
            trigrams_counter = Counter(trigrams)
            sentences_counter = Counter(sentences)

            if len(words):
                text_analysis.word_type_token_ratio = len(words_counter) / len(words) * 100
                text_analysis.word_count = len(words)
                text_analysis.word_count_unique = len(words_counter)
            del words_counter

            if len(bigrams):
                text_analysis.bigram_type_token_ratio = len(bigrams_counter) / len(bigrams) * 100
                text_analysis.bigram_count = len(bigrams)
                text_analysis.bigram_count_unique = len(bigrams_counter)
            del bigrams_counter

            if len(trigrams):
                text_analysis.trigram_type_token_ratio = len(trigrams_counter) / len(trigrams) * 100
                text_analysis.trigram_count = len(trigrams)
                text_analysis.trigram_count_unique = len(trigrams_counter)
            del trigrams_counter

            if len(sentences):
                sentence_type_token_ratio = len(sentences_counter) / len(sentences) * 100
                text_analysis.sentence_type_token_ratio = sentence_type_token_ratio

                text_analysis.sentence_count = len(sentences)
                text_analysis.sentence_count_unique = len(sentences_counter)
            del sentences_counter

            #
            # Compute average sentence length
            #
            if len(sentences):
                sentences_char_count = sum([len(sentence) for sentence in sentences])
                text_analysis.sentence_average_length = sentences_char_count / len(sentences)

            #
            # Compute char counts
            #
            text_analysis.char_count = len(merged_text)

            text_analysis.char_count_continous = len(
                merged_text.replace(" ", "")
                .replace("\n", "")
                .replace("\t", "")
                .replace("\u200b", "")
                .replace("-", "")
                .replace("—", "")
            )

            #
            # Compute "tokenizability" (how "well" the words in this text tokenize)
            #
            total_word_tokens = 0

            for word in words:
                total_word_tokens += len(tokenizer.encode(word))

            if total_word_tokens:
                text_analysis.tokenizability_o200k_base_ratio = (
                    len(words) * 1.25 / total_word_tokens * 100
                )

            # NOTE: Score may exceed 100 because of our 1 token = 1.25 word target
            if text_analysis.tokenizability_o200k_base_ratio > 100.0:
                text_analysis.tokenizability_o200k_base_ratio = 100.0

            click.echo(f"🧮 #{book.barcode} processed in {datetime.now() - start_datetime}.")

        #
        # Add to batch
        #
        if already_exists:
            entries_to_update.append(text_analysis)
        else:
            entries_to_create.append(text_analysis)

    #
    # Save batches
    #
    process_db_write_batch(
        model,
        entries_to_create,
        entries_to_update,
        [
            model.char_count,
            model.char_count_continous,
            model.word_count,
            model.word_count_unique,
            model.word_type_token_ratio,
            model.bigram_count,
            model.bigram_count_unique,
            model.bigram_type_token_ratio,
            model.trigram_count,
            model.trigram_count_unique,
            model.trigram_type_token_ratio,
            model.sentence_count,
            model.sentence_count_unique,
            model.sentence_type_token_ratio,
            model.sentence_average_length,
            model.tokenizability_o200k_base_ratio,
        ],
    )
