import csv
from pathlib import Path

import click
import peewee

import utils
from models import (
    BookIO,
    PageCount,
    MainLanguage,
    LanguageDetection,
    TokenCount,
    HathitrustRightsDetermination,
    YearOfPublication,
    OCRQuality,
    TextAnalysis,
    ScannedTextSimhash,
)
from const import OUTPUT_EXPORT_DIR_PATH, DATETIME_SLUG


@click.command("overview")
@utils.needs_pipeline_ready
def overview():
    """
    Generates a CSV with high-level stats about the data collected by this pipeline.

    Saved as:
    - `/data/output/export/overview-{datetime}.csv`
    """
    output_filepath = Path(OUTPUT_EXPORT_DIR_PATH, f"overview-{DATETIME_SLUG}.csv")

    with open(output_filepath, "w+") as fd:
        writer = csv.writer(fd)

        books_stats(writer)
        token_count_stats(writer)
        page_count_stats(writer)
        main_language_stats(writer)
        language_detection_stats(writer)
        rights_determination_stats(writer)
        year_of_publication_stats(writer)
        ocr_quality_stats(writer)
        deduplication_stats(writer)
        text_analysis_stats(writer)

        # Genres classification
        # Topic classification (+ split by book, + average confidence)
        # Layout-aware text stats

    click.echo(f"✅ {output_filepath.name} saved to disk.")


def books_stats(writer: csv.writer):
    insert_section(writer, "BOOKS")

    insert_row(
        writer,
        "Total books in collection",
        BookIO.select().count(),
    )

    insert_row(
        writer,
        "Total books with text",
        PageCount.select().where(PageCount.count_from_ocr > 0).count(),
    )

    insert_row(
        writer,
        "Total books without text",
        PageCount.select().where(PageCount.count_from_ocr < 1).count(),
    )


def token_count_stats(writer: csv.writer):
    insert_section(writer, "TOKEN COUNT")

    target_llms_query = (
        TokenCount.select(TokenCount.target_llm).distinct().order_by(TokenCount.target_llm.desc())
    )

    for target_llm in [token_count.target_llm for token_count in target_llms_query]:
        insert_row(
            writer,
            f"{target_llm} - Total token count",
            (
                TokenCount.select(peewee.fn.SUM(TokenCount.count))
                .where(TokenCount.target_llm == target_llm)
                .scalar()
            ),
        )

        insert_row(
            writer,
            f"{target_llm} - Average token count",
            (
                TokenCount.select(peewee.fn.AVG(TokenCount.count))
                .where(TokenCount.target_llm == target_llm)
                .scalar()
            ),
        )


def page_count_stats(writer: csv.writer):
    insert_section(writer, "PAGE COUNT")

    insert_row(
        writer,
        "Total pages",
        PageCount.select(peewee.fn.SUM(PageCount.count_from_ocr)).scalar(),
    )

    insert_row(
        writer,
        "Average page count",
        PageCount.select(peewee.fn.AVG(PageCount.count_from_ocr)).scalar(),
    )


def main_language_stats(writer: csv.writer):
    insert_section(writer, "BOOK-LEVEL MAIN LANGUAGE")

    insert_row(
        writer,
        "Total unique main languages from metadata",
        (
            MainLanguage.select(MainLanguage.from_metadata_iso693_2b)
            .where(MainLanguage.from_metadata_iso693_2b.is_null(False))
            .distinct()
            .count()
        ),
    )

    insert_row(
        writer,
        "Total unique main languages from detection",
        (
            MainLanguage.select(MainLanguage.from_detection_iso693_3)
            .where(MainLanguage.from_detection_iso693_3.is_null(False))
            .distinct()
            .count()
        ),
    )

    insert_row(
        writer,
        "Total books with no main language metadata",
        (
            MainLanguage.select()
            .where(
                MainLanguage.from_metadata_iso693_2b.is_null(True),
            )
            .count()
        ),
    )

    insert_row(
        writer,
        "Total books with no main language from detection",
        (
            MainLanguage.select()
            .where(
                MainLanguage.from_detection_iso693_3.is_null(True),
            )
            .count()
        ),
    )

    # Books by main language - from metadata
    languages_from_metadata_query = (
        MainLanguage.select(MainLanguage.from_metadata_iso693_3)
        .where(MainLanguage.from_metadata_iso693_3.is_null(False))
        .order_by(MainLanguage.from_metadata_iso693_3)
        .distinct()
    )

    languages_from_detection_query = (
        MainLanguage.select(MainLanguage.from_detection_iso693_3)
        .where(MainLanguage.from_detection_iso693_3.is_null(False))
        .order_by(MainLanguage.from_detection_iso693_3)
        .distinct()
    )

    for lang in [entry.from_metadata_iso693_3 for entry in languages_from_metadata_query]:

        if not lang.strip():
            continue

        insert_row(
            writer,
            f"(Metadata) Total {lang} books",
            MainLanguage.select().where(MainLanguage.from_metadata_iso693_3 == lang).count(),
            lang,
        )

    for lang in [entry.from_detection_iso693_3 for entry in languages_from_detection_query]:

        if not lang.strip():
            continue

        insert_row(
            writer,
            f"(Detection) Total {lang} books",
            MainLanguage.select().where(MainLanguage.from_detection_iso693_3 == lang).count(),
            lang,
        )


def rights_determination_stats(writer: csv.writer):
    insert_section(writer, "RIGHTS DETERMINATION")

    writer.writerow(
        [
            "Total books matched with Hathitrust records",
            HathitrustRightsDetermination.select().count(),
        ]
    )

    rights_codes_query = (
        HathitrustRightsDetermination.select(HathitrustRightsDetermination.rights_code)
        .where(HathitrustRightsDetermination.rights_code.is_null(False))
        .distinct()
        .order_by(HathitrustRightsDetermination.rights_code.desc())
    )

    for rights_code in [entry.rights_code for entry in rights_codes_query]:
        insert_row(
            writer,
            f"Total books flagged as {rights_code.upper()} by Hathitrust",
            (
                HathitrustRightsDetermination.select()
                .where(HathitrustRightsDetermination.rights_code == rights_code)
                .count()
            ),
            rights_code,
        )

    insert_row(
        writer,
        f"Total books with no rights code given by Hathitrust",
        (
            HathitrustRightsDetermination.select()
            .where(HathitrustRightsDetermination.rights_code.is_null(True))
            .count()
        ),
    )

    insert_row(
        writer,
        f'Total books flagged as either PD, PDUS or CC-ZERO and "Full view" by Hathitrust',
        (
            HathitrustRightsDetermination.select()
            .where(
                (HathitrustRightsDetermination.rights_code in ["pd", "pdus", "cc-zero"])
                & (HathitrustRightsDetermination.us_rights_string == "Full view")
            )
            .count()
        ),
    )

    insert_row(
        writer,
        f'Total books flagged as either PD, PDUS or CC-ZERO and "Full view" by Hathitrust that have text',
        (
            HathitrustRightsDetermination.select()
            .where(
                (HathitrustRightsDetermination.rights_code in ["pd", "pdus", "cc-zero"])
                & (HathitrustRightsDetermination.us_rights_string == "Full view")
                & (
                    HathitrustRightsDetermination.book.in_(
                        PageCount.select(PageCount.book).where(PageCount.count_from_ocr > 0)
                    )
                )
            )
            .count()
        ),
    )


def year_of_publication_stats(writer: csv.writer):
    centuries_query = (
        YearOfPublication.select(YearOfPublication.century)
        .where(YearOfPublication.century.is_null(False))
        .distinct()
        .order_by(YearOfPublication.century)
    )

    decades_query = (
        YearOfPublication.select(YearOfPublication.decade)
        .where(YearOfPublication.decade.is_null(False))
        .distinct()
        .order_by(YearOfPublication.decade)
    )

    # Books by century
    insert_section(writer, "REPORTED PUBLICATION DATES - BY CENTURY")

    for century in [entry.century for entry in centuries_query]:

        if century > 2000:
            continue

        insert_row(
            writer,
            f"(by century) Total books with a reported publication date in the {century}s",
            YearOfPublication.select().where(YearOfPublication.century == century).count(),
            century,
        )

    # Books by decade
    insert_section(writer, "REPORTED PUBLICATION DATES - BY DECADE")

    for decade in [entry.decade for entry in decades_query]:

        if decade > 2100:
            continue

        insert_row(
            writer,
            f"(by decade) Total books with a reported publication date in the {decade}s",
            YearOfPublication.select().where(YearOfPublication.decade == decade).count(),
            decade,
        )

    # No date or invalid date
    insert_section(writer, "REPORTED PUBLICATION DATES - CONTINUED")

    insert_row(
        writer,
        f"Total books with no known or invalid publication date",
        (
            YearOfPublication.select()
            .where(YearOfPublication.year.is_null(True) or YearOfPublication.year > 2100)
            .count()
        ),
    )


def ocr_quality_stats(writer: csv.writer):
    insert_section(writer, "OCR QUALITY")

    # Average
    insert_row(
        writer,
        f"Google Books-provided - Average",
        OCRQuality.select(peewee.fn.AVG(OCRQuality.from_metadata)).scalar(),
    )

    insert_row(
        writer,
        f"pleias/OCRoscope - Average",
        OCRQuality.select(peewee.fn.AVG(OCRQuality.from_detection)).scalar(),
    )

    # No data
    insert_row(
        writer,
        f"Google Books-provided - Book with no score available",
        OCRQuality.select().where(OCRQuality.from_metadata.is_null(True)).count(),
    )

    insert_row(
        writer,
        f"pleias/OCRoscope - Book with no score available",
        OCRQuality.select().where(OCRQuality.from_detection.is_null(True)).count(),
    )

    # Average by decade
    decades_query = (
        YearOfPublication.select(YearOfPublication.decade)
        .where(YearOfPublication.decade.is_null(False))
        .distinct()
        .order_by(YearOfPublication.decade)
    )

    for decade in [entry.decade for entry in decades_query]:
        if decade > 2100:
            continue

        insert_row(
            writer,
            f"(by decade) Google Books-provided - Average for books likely published in the {decade}s",
            (
                OCRQuality.select(peewee.fn.AVG(OCRQuality.from_metadata))
                .join(YearOfPublication, on=(OCRQuality.book == YearOfPublication.book))
                .where(YearOfPublication.decade == decade)
                .scalar()
            ),
            decade,
        )

    for decade in [entry.decade for entry in decades_query]:

        if decade > 2100:
            continue

        insert_row(
            writer,
            f"(by decade) pleias/OCRoscope - Average for books likely published in the {decade}s",
            (
                OCRQuality.select(peewee.fn.AVG(OCRQuality.from_metadata))
                .join(YearOfPublication, on=(OCRQuality.book == YearOfPublication.book))
                .where(YearOfPublication.decade == decade)
                .scalar()
            ),
            decade,
        )


def language_detection_stats(writer: csv.writer):
    insert_section(writer, "LANGUAGE DETECTION")

    insert_row(
        writer,
        f"Total unique languages detected at text level (> 1000 tokens)",
        (
            LanguageDetection.select(LanguageDetection.iso693_3)
            .where(
                (LanguageDetection.iso693_3.is_null(False)) & (LanguageDetection.token_count > 1000)
            )
            .distinct()
            .count()
        ),
    )


def deduplication_stats(writer: csv.writer):
    insert_section(writer, "COLLECTION-LEVEL DEDUPLICATION")

    hashes_to_books = utils.get_filtered_duplicates()

    simhashes = set()

    total_unique_simhashes = (
        ScannedTextSimhash.select().where(ScannedTextSimhash.hash.is_null(False)).distinct().count()
    )

    total_books_with_simhash = (
        ScannedTextSimhash.select().where(ScannedTextSimhash.hash.is_null(False)).count()
    )

    total_unique_books_with_dupes = 0
    total_books_with_at_least_one_dupe = 0

    for books in hashes_to_books.values():

        if len(books) > 1:
            total_unique_books_with_dupes += 1
            total_books_with_at_least_one_dupe += len(books)

    insert_row(
        writer,
        "Total books with simhash",
        total_books_with_simhash,
    )

    insert_row(
        writer,
        "Total unique simhashes",
        total_unique_simhashes,
    )

    insert_row(
        writer,
        "(filtered) Total books with at least one duplicate",
        total_books_with_at_least_one_dupe,
    )

    insert_row(
        writer,
        "(filtered) Total unique books in duplicate set",
        total_unique_books_with_dupes,
    )

    insert_row(
        writer,
        "(filtered) Total unique books (with text) in the collection collection",
        (
            total_books_with_simhash
            - total_books_with_at_least_one_dupe
            + total_unique_books_with_dupes
        ),
    )


def text_analysis_stats(writer: csv.writer):
    insert_section(writer, "TEXT ANALYSIS")

    # Characters
    insert_row(
        writer,
        f"Average character count",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.char_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average continuous character count",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.char_count_continous)).scalar(),
    )

    # Words
    insert_row(
        writer,
        f"Average word count",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.word_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique words",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.word_count_unique)).scalar(),
    )

    insert_row(
        writer,
        f"Average word type-token ratio",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.word_type_token_ratio)).scalar(),
    )

    # Bigrams
    insert_row(
        writer,
        f"Average bigram count",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.bigram_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique bigrams",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.bigram_count_unique)).scalar(),
    )

    insert_row(
        writer,
        f"Average bigram type-token ratio",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.bigram_type_token_ratio)).scalar(),
    )

    # Trigrams
    insert_row(
        writer,
        f"Average trigram count",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.trigram_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique tigrams",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.trigram_count_unique)).scalar(),
    )

    insert_row(
        writer,
        f"Average trigram type-token ratio",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.trigram_type_token_ratio)).scalar(),
    )

    # Sentences
    insert_row(
        writer,
        f"Average sentence count",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.sentence_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique sentences",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.sentence_count_unique)).scalar(),
    )

    # Tokenizability (o200k_base)
    insert_row(
        writer,
        f"Average tokenizability",
        TextAnalysis.select(peewee.fn.AVG(TextAnalysis.tokenizability_o200k_base_ratio)).scalar(),
    )


def insert_section(writer: csv.writer, title: str):
    writer.writerow([" ", " ", " "])
    writer.writerow([title, " ", " "])


def insert_row(writer: csv.writer, caption: str, value="", extra=""):
    writer.writerow([caption, value, extra])
