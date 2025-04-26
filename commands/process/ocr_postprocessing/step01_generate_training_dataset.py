from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import random

import click
import peewee
import numpy as np
from openai import OpenAI

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

TARGET_MODEL = ""
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

## Notes that you can use as hints:
- Running heads are generally in the first few chunks (0 to 10). They can be hard to distinguish from headings but their proximity to a page number can help.
- The very beginning and the end of a book are less likely to contain footnotes and running heads.
- There is likely only 1 page number per page, and it is either at the top (first 5 chunks) or bottom (last 5 chunks) of the page.
- Footnotes sometimes start with a number, and are sometimes wrapped in parenthesis. They are generally towards the end of the page.
- When working on paragraphs or footnotes, carefully analyze sentences to determine if it is the beginning, the end, or any chunk of it.

## Possible values for TYPE:
- {"\n- ".join(TARGET_TYPES)}

## Examples:

```
<context>Page 104 of 344, Chunk 1 of 47, Language: eng</context>
<current>50</current>
<next>Records of the Geological Survey of New South Wales. [VOL. IV.</next>
```
is PAGE_NUMBER 

```
<context>Page 104 of 344, Chunk 2 of 47, Language: eng</context>
<previous>50</previous>
<current>Records of the Geological Survey of New South Wales. [VOL. IV.</current>
<next>The lanceolate or almost boat-shaped form is a very characteristic feature, and</next>
```
is RUNNING_HEAD 


```
<context>Page 104 of 344, Chunk 3 of 47, Language: eng</context>
<previous>Records of the Geological Survey of New South Wales. [VOL. IV.</previous>
<current>The lanceolate or almost boat-shaped form is a very characteristic feature, and</current>
<next>seems to vary but little amongst a large number of specimens, the more club-</next>
```
is PARAGRAPH_START

```
<context>Page 104 of 344, Chunk 4 of 47, Language: eng</context>
<previous>The lanceolate or almost boat-shaped form is a very characteristic feature, and</previous>
<current>seems to vary but little amongst a large number of specimens, the more club-</current>
<next>shaped outline of Fig. 2 arising from a cause before explained. The margins are</next>
```
is PARAGRAPH_CHUNK

```
<context>Page 104 of 344, Chunk 19 of 47, Language: eng</context>
<previous>wing of the leaf. So far as observation has gone the bifurcation is unifurcate in</previous>
<current>respective veins.</current>
<next>We may now compare the leaves of O. lentriculiforme with those of some of its</next>

```
is PARAGRAPH_END 

```
<context>Page 165 of 492, Chunk 10 of 41, Language: eng</context>
<previous>among the rich and the noble. The rich and the</previous>
<current>noble are not impelled to intellectual exertion by</current>
<next>necessity. They may be impelled to intellectual</next>
```
is PARAGRAPH_CHUNK


```
<context>Page 65 of 364, Chunk 1 of 44, Language: eng</context>
<current>CHAP. II. §4.] OF CONVICTION.</current>
<next>59</next>
```
is RUNNING_HEAD

```
<context>Page 65 of 364, Chunk 2 of 44, Language: eng</context>
<previous>CHAP. II. §4.] OF CONVICTION.</previous>
<current>59</current>
<next>This observation is the more important, because</next>
```
is PAGE_NUMBER


```
<context>Page 65 of 364, Chunk 24 of 44, Language: eng</context>
<previous>thought it false-so long unrefuted, or else, denying</previous>
<current>what they knew to be true.</current>
<next>Misrepresentation, again, of argument-attempts to</next>
```
is PARAGRAPH_END 


```
<context>Page 65 of 364, Chunk 25 of 44, Language: eng</context>
<previous>what they knew to be true.</previous>
<current>Misrepresentation, again, of argument-attempts to</current>
<next>suppress evidence, or to silence a speaker by clamour</next>
```
is PARAGRAPH_START

```
<context>Page 331 of 646, Chunk 32 of 54, Language: ita</context>
<previous>priv. u. öff. Recht, 1898, XXVI, p. 19 e s.; - VIVANTE, Contr. d'assicur., 1887,</previous>
<current>III, n. 50.</current>
<next>-</next>
```
is HEADING_FULL

```
<context>Page 331 of 646, Chunk 33 of 54, Language: ita</context>
<previous>III, n. 50.</previous>
<current>-</current>
<next>---</next>
```
is NOISE_OR_BROKEN_TEXT

```
<context>Page 331 of 646, Chunk 34 of 54, Language: ita</context>
<previous>-</previous>
<current>---</current>
<next>―</next>
```
is SEPARATOR


```
<context>Page 331 of 646, Chunk 36 of 54, Language: ita</context>
<previous>―</previous>
<current>(2) App. Parigi, 12 febbraio 1857, Sirey, 1857, 2, 186 (dapprima partendo</current>
<next>dall'esser commerciale il titolo, riguardo all'assicuratore almeno; in seguito</next>
```
is FOOTNOTE_START

```
<context>Page 331 of 646, Chunk 40 of 54, Language: ita</context>
<previous>biglietti all'ordine emessi da non commercianti); — Id., 2 aprile 1879, Dalloz,</previous>
<current>1879, 2, 130; — Id., 8 giugno 1899, Dalloz, 1900, 2, 11; Cass. fr., 22 giu-</current>
<next>gno 1891, Sirey, 1892, 1, 177; Id., 6 maggio 1891, Dalloz, 1893, 1, 179;</next>
```
is FOOTNOTE_CHUNK


```
<context>Page 491 of 832, Chunk 16 of 47, Language: eng</context>
<previous>ment of $102, and judgment was rendered for the balance.</previous>
<current>Section 907b provides:</current>
<next>"Nor shall any judgment, the record whereof has been</next>
```
is PARAGRAPH_START


```
<context>Page 240 of 328, Chunk 21 of 157, Language: eng</context>
<previous>1.</previous>
<current>2</current>
<next>12,410</next>
```
is NOISE_OR_BROKEN_TEXT
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
    help="ISO693_3 code of the languages to focus on. By default, focuses on the top 5 languages.",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=32,
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
    """
    books = []
    books_chunks = {}

    train_cap = 0
    train_count = 0
    test_cap = 0
    test_count = 0

    if n_samples < 25:
        click.echo("Cannot generate a set with less than 50 samples.")
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
    # Process batches of chunks in parallel
    #
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for barcode, chunks in books_chunks.items():
            future = executor.submit(process_page_chunks, chunks)
            futures[future] = barcode

        for future in as_completed(futures):
            barcode = futures[future]

            try:
                chunks = future.result()
                books_chunks[barcode] = chunks
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not generate OCR postprocessing training set. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)

    #
    # Save all chunks
    #
    for barcode, chunks in books_chunks.items():
        utils.process_db_write_batch(OCRPostprocessingTrainingDataset, chunks, [], [])

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
            auto_annotation_repr = OCRPostprocessingTrainingDataset.get_auto_annotation_repr(
                current,
                previous,
                next,
                len(chunks),
            )

            assign_ocr_chunk_type(current, auto_annotation_repr)
        except Exception:
            click.echo(traceback.format_exc())
            click.echo(
                f"Couldn't generate training data for chunk {current.get_training_repr()}. Skipping."
            )

    return chunks


def assign_ocr_chunk_type(
    chunk: OCRPostprocessingTrainingDataset,
    auto_annotation_repr: str,
) -> OCRPostprocessingTrainingDataset:
    """
    Calls the OpenAI (TRAINING_SET_GENERATION_SYSTEM_PROMPT on TARGET_MODEL) to annotate an OCR Chunk.
    """
    target_type = ""
    perplexity = 0.0
    average_linear_logprob = 0.0

    # Run completion
    completion = OpenAI().chat.completions.create(
        messages=[
            {"role": "system", "content": TRAINING_SET_GENERATION_SYSTEM_PROMPT},
            {"role": "user", "content": auto_annotation_repr},
        ],
        model=TARGET_MODEL,
        temperature=0.0,
        max_tokens=25,
        logprobs=True,
        top_logprobs=1,
    )

    # Grab target type, check if its valid
    target_type = completion.choices[0].message.content
    target_type = target_type.strip()

    assert target_type in TARGET_TYPES

    # Compute average linear probability and perplexity
    # https://cookbook.openai.com/examples/using_logprobs
    logprobs = [token.logprob for token in completion.choices[0].logprobs.content]

    perplexity = np.exp(-np.mean(logprobs))

    for logprob in logprobs:
        linear_prob = np.round(np.exp(logprob) * 100, 2)
        average_linear_logprob += linear_prob

    average_linear_logprob = np.round(average_linear_logprob / len(logprobs), 2)

    # Update OCR and return OCR Chunk
    chunk.target_type = target_type
    chunk.target_type_average_linear_logprob = average_linear_logprob
    chunk.target_type_perplexity = perplexity

    click.echo(
        f"{chunk.get_training_repr()} -> {chunk.target_type} ({chunk.target_type_average_linear_logprob})"
    )

    return chunk
