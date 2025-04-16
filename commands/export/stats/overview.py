import csv
from pathlib import Path

import click
from peewee import fn

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
    GenreClassification,
    TopicClassification,
    TopicClassificationTrainingDataset,
)
from const import OUTPUT_EXPORT_DIR_PATH, DATETIME_SLUG


@click.command("overview")
@utils.needs_pipeline_ready
def overview():
    """
    Generates a CSV with stats about the data collected by this pipeline.

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
        genre_classification_stats(writer)
        topic_classification_stats(writer)
        layout_aware_text_stats(writer)

    click.echo(f"✅ {output_filepath.name} saved to disk.")


def insert_section(writer: csv.writer, title: str):
    """
    Inserts a "title" in the CSV with an empty line as a separator.
    """
    writer.writerow([" ", " ", " "])
    writer.writerow([title, " ", " "])


def insert_row(writer: csv.writer, caption: str, value="", extra=""):
    """
    Shortcut: writes a row to CSV.
    """
    writer.writerow([caption, value, extra])


def books_stats(writer: csv.writer):
    """
    Writes books-related stats to CSV.
    """
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
    """
    Writes token count-related stats to CSV.
    """
    # Total tokens
    insert_section(writer, "TOKEN COUNT (TOTAL)")

    for item in (
        TokenCount.select(
            TokenCount.target_llm,
            fn.SUM(TokenCount.count).alias("total"),
        )
        .group_by(TokenCount.target_llm)
        .order_by(TokenCount.target_llm.desc())
    ):
        insert_row(
            writer,
            f"{item.target_llm} - Total token count",
            item.total,
            item.target_llm,
        )

    # Average token count
    insert_section(writer, "TOKEN COUNT (PER-BOOK AVERAGE)")

    for item in (
        TokenCount.select(
            TokenCount.target_llm,
            fn.AVG(TokenCount.count).alias("average"),
        )
        .group_by(TokenCount.target_llm)
        .order_by(TokenCount.target_llm.desc())
    ):
        insert_row(
            writer,
            f"{item.target_llm} - Average token count",
            item.average,
            item.target_llm,
        )


def page_count_stats(writer: csv.writer):
    """
    Writes page count-related stats to CSV.
    """
    insert_section(writer, "PAGE COUNT")

    insert_row(
        writer,
        "Total pages",
        PageCount.select(fn.SUM(PageCount.count_from_ocr)).scalar(),
    )

    insert_row(
        writer,
        "Average page count",
        PageCount.select(fn.AVG(PageCount.count_from_ocr)).scalar(),
    )


def main_language_stats(writer: csv.writer):
    """
    Writes main language-related stats to CSV.
    """
    #
    # High-level
    #
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
        "Total books with no main language from metadata",
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

    #
    # Total by language - from metadata
    #
    insert_section(writer, "BOOK-LEVEL MAIN LANGUAGE - TOTAL BOOKS - SOURCE: METADATA")

    for item in (
        MainLanguage.select(
            MainLanguage.from_metadata_iso693_3,
            fn.COUNT(MainLanguage.from_metadata_iso693_3).alias("total"),
        )
        .where(MainLanguage.from_metadata_iso693_3.is_null(False))
        .group_by(MainLanguage.from_metadata_iso693_3)
        .order_by(MainLanguage.from_metadata_iso693_3)
    ):
        if not item.from_metadata_iso693_3.strip():
            continue

        insert_row(
            writer,
            f"Total {item.from_metadata_iso693_3} books",
            item.total,
            item.from_metadata_iso693_3,
        )

    #
    # Total by language - from detection
    #
    insert_section(writer, "BOOK-LEVEL MAIN LANGUAGE - TOTAL BOOKS - SOURCE: DETECTION")

    for item in (
        MainLanguage.select(
            MainLanguage.from_detection_iso693_3,
            fn.COUNT(MainLanguage.from_detection_iso693_3).alias("total"),
        )
        .where(MainLanguage.from_detection_iso693_3.is_null(False))
        .group_by(MainLanguage.from_detection_iso693_3)
        .order_by(MainLanguage.from_detection_iso693_3)
    ):
        if not item.from_detection_iso693_3.strip():
            continue

        insert_row(
            writer,
            f"Total {item.from_detection_iso693_3} books",
            item.total,
            item.from_detection_iso693_3,
        )


def rights_determination_stats(writer: csv.writer):
    """
    Writes rights determination-related stats to CSV.
    """
    insert_section(writer, "RIGHTS DETERMINATION - OVERVIEW")

    insert_row(
        writer,
        "Total books matched with Hathitrust records",
        HathitrustRightsDetermination.select().count(),
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

    #
    # Breakdown
    #
    insert_section(writer, "RIGHTS DETERMINATION - BREAKDOWN")

    for item in (
        HathitrustRightsDetermination.select(
            HathitrustRightsDetermination.rights_code,
            fn.COUNT(HathitrustRightsDetermination.rights_code).alias("total"),
        )
        .where(HathitrustRightsDetermination.rights_code.is_null(False))
        .group_by(HathitrustRightsDetermination.rights_code)
        .order_by(HathitrustRightsDetermination.rights_code.desc())
    ):
        insert_row(
            writer,
            f"Total books flagged as {item.rights_code.upper()} by Hathitrust",
            item.total,
            item.rights_code,
        )


def year_of_publication_stats(writer: csv.writer):
    """
    Writes year of publication-related stats to CSV.
    """
    #
    # Overview
    #
    insert_section(writer, "REPORTED PUBLICATION DATES - OVERVIEW")

    insert_row(
        writer,
        f"Total books with known / valid publication date",
        (
            YearOfPublication.select()
            .where(YearOfPublication.year.is_null(False) and YearOfPublication.year < 2025)
            .count()
        ),
    )

    insert_row(
        writer,
        f"Total books with no known or invalid publication date",
        (
            YearOfPublication.select()
            .where(YearOfPublication.year.is_null(True) or YearOfPublication.year > 2025)
            .count()
        ),
    )

    #
    # Books by century
    #
    insert_section(writer, "REPORTED PUBLICATION DATES - BY CENTURY")

    for item in (
        YearOfPublication.select(
            YearOfPublication.century,
            fn.COUNT(YearOfPublication.book_id).alias("total"),
        )
        .where(YearOfPublication.century.is_null(False) & YearOfPublication.year < 2030)
        .group_by(YearOfPublication.century)
        .order_by(YearOfPublication.century)
    ):

        if not item.century:
            continue

        if item.century > 2000:
            continue

        insert_row(
            writer,
            f"Total books with a reported publication date in the {item.century}s",
            item.total,
            item.century,
        )

    #
    # Books by decade
    #
    insert_section(writer, "REPORTED PUBLICATION DATES - BY DECADE")

    for item in (
        YearOfPublication.select(
            YearOfPublication.decade,
            fn.COUNT(YearOfPublication.book_id).alias("total"),
        )
        .where(YearOfPublication.decade.is_null(False))
        .group_by(YearOfPublication.decade)
        .order_by(YearOfPublication.decade)
    ):
        if not item.decade:
            continue

        if item.decade > 2020:
            continue

        insert_row(
            writer,
            f"Total books with a reported publication date in the {item.decade}s",
            item.total,
            item.decade,
        )


def ocr_quality_stats(writer: csv.writer):
    """
    Writes OCR quality-related stats to CSV.
    """
    decades_query = (
        YearOfPublication.select(YearOfPublication.decade)
        .where(YearOfPublication.decade.is_null(False))
        .distinct()
        .order_by(YearOfPublication.decade)
    )

    #
    # From metadata
    #
    insert_section(writer, "OCR QUALITY - OVERVIEW")

    # Average
    insert_row(
        writer,
        f"Google Books-provided - Average",
        OCRQuality.select(fn.AVG(OCRQuality.from_metadata)).scalar(),
    )

    insert_row(
        writer,
        f"pleias/OCRoscope - Average",
        OCRQuality.select(fn.AVG(OCRQuality.from_detection)).scalar(),
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

    #
    # From metadata
    #
    insert_section(writer, "OCR QUALITY - METADATA - BY DECADE AVERAGE")

    for decade in [entry.decade for entry in decades_query]:

        if decade > 2020:
            continue

        insert_row(
            writer,
            f"OCR Quality average for books likely published in the {decade}s",
            (
                OCRQuality.select(fn.AVG(OCRQuality.from_metadata))
                .join(YearOfPublication, on=(OCRQuality.book == YearOfPublication.book))
                .where(YearOfPublication.decade == decade)
                .scalar()
            ),
            decade,
        )

    #
    # From detection
    #
    insert_section(writer, "OCR QUALITY - DETECTION - BY DECADE AVERAGE")

    for decade in [entry.decade for entry in decades_query]:

        if decade > 2020:
            continue

        insert_row(
            writer,
            f"OCR Quality average for books likely published in the {decade}s",
            (
                OCRQuality.select(fn.AVG(OCRQuality.from_detection))
                .join(YearOfPublication, on=(OCRQuality.book == YearOfPublication.book))
                .where(YearOfPublication.decade == decade)
                .scalar()
            ),
            decade,
        )


def language_detection_stats(writer: csv.writer):
    """
    Writes text-level language detection-related stats to CSV.
    """
    #
    # Overview
    #
    insert_section(writer, "TEXT-LEVEL LANGUAGE DETECTION - OVERVIEW ")

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

    #
    # Overview
    #
    insert_section(
        writer,
        "TEXT-LEVEL LANGUAGE DETECTION - BREAKDOWN - ALL DETECTION > 1000 TOKENS",
    )

    for lang in (
        LanguageDetection.select(
            LanguageDetection.iso693_3,
            fn.SUM(LanguageDetection.token_count).alias("total_tokens"),
        )
        .where((LanguageDetection.iso693_3.is_null(False)) & (LanguageDetection.token_count > 1000))
        .group_by(LanguageDetection.iso693_3)
        .order_by(LanguageDetection.iso693_3)
    ):
        insert_row(
            writer,
            f"Total detected tokens for {lang.iso693_3} (anything > 1000)",
            lang.total_tokens,
            lang.iso693_3,
        )


def deduplication_stats(writer: csv.writer):
    """
    Writes collection-level deduplication-related stats to CSV.
    """
    insert_section(writer, "COLLECTION-LEVEL DEDUPLICATION")

    hashes_to_books = utils.get_filtered_duplicates()

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
    """
    Writes text analysis-related stats to CSV.
    """
    insert_section(writer, "TEXT ANALYSIS")

    # Characters
    insert_row(
        writer,
        f"Average character count",
        TextAnalysis.select(fn.AVG(TextAnalysis.char_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average continuous character count",
        TextAnalysis.select(fn.AVG(TextAnalysis.char_count_continous)).scalar(),
    )

    # Words
    insert_row(
        writer,
        f"Average word count",
        TextAnalysis.select(fn.AVG(TextAnalysis.word_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique words",
        TextAnalysis.select(fn.AVG(TextAnalysis.word_count_unique)).scalar(),
    )

    insert_row(
        writer,
        f"Average word type-token ratio",
        TextAnalysis.select(fn.AVG(TextAnalysis.word_type_token_ratio)).scalar(),
    )

    # Bigrams
    insert_row(
        writer,
        f"Average bigram count",
        TextAnalysis.select(fn.AVG(TextAnalysis.bigram_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique bigrams",
        TextAnalysis.select(fn.AVG(TextAnalysis.bigram_count_unique)).scalar(),
    )

    insert_row(
        writer,
        f"Average bigram type-token ratio",
        TextAnalysis.select(fn.AVG(TextAnalysis.bigram_type_token_ratio)).scalar(),
    )

    # Trigrams
    insert_row(
        writer,
        f"Average trigram count",
        TextAnalysis.select(fn.AVG(TextAnalysis.trigram_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique tigrams",
        TextAnalysis.select(fn.AVG(TextAnalysis.trigram_count_unique)).scalar(),
    )

    insert_row(
        writer,
        f"Average trigram type-token ratio",
        TextAnalysis.select(fn.AVG(TextAnalysis.trigram_type_token_ratio)).scalar(),
    )

    # Sentences
    insert_row(
        writer,
        f"Average sentence count",
        TextAnalysis.select(fn.AVG(TextAnalysis.sentence_count)).scalar(),
    )

    insert_row(
        writer,
        f"Average unique sentences",
        TextAnalysis.select(fn.AVG(TextAnalysis.sentence_count_unique)).scalar(),
    )

    # Tokenizability (o200k_base)
    insert_row(
        writer,
        f"Average tokenizability",
        TextAnalysis.select(fn.AVG(TextAnalysis.tokenizability_o200k_base_ratio)).scalar(),
    )


def genre_classification_stats(writer: csv.writer):
    """
    Writes genre classification-related stats to CSV.
    """
    #
    # Overview
    #
    insert_section(writer, "GENRE/FORM CLASSIFICATION - FROM METADATA - OVERVIEW")

    insert_row(
        writer,
        "Total books with genre/form info from metadata",
        (
            GenreClassification.select()
            .where(GenreClassification.from_metadata.is_null(False))
            .count()
        ),
    )

    insert_row(
        writer,
        "Total number of unique values for genre/form assigned from metadata",
        (
            GenreClassification.select(GenreClassification.from_metadata)
            .where(GenreClassification.from_metadata.is_null(False))
            .distinct()
            .count()
        ),
    )

    #
    # Top 50
    #
    insert_section(writer, "GENRE/FORM CLASSIFICATION - METADATA - TOP 50")

    for i, item in enumerate(
        GenreClassification.select(
            GenreClassification.from_metadata,
            fn.COUNT(GenreClassification.from_metadata).alias("total"),
        )
        .where(GenreClassification.from_metadata.is_null(False))
        .group_by(GenreClassification.from_metadata)
        .order_by(fn.COUNT(GenreClassification.from_metadata).desc())
    ):

        if i >= 50:
            break

        insert_row(
            writer,
            f"Books labeled as {item.from_metadata}",
            item.total,
            item.from_metadata,
        )


def topic_classification_stats(writer: csv.writer):
    """
    Writes topic classification-related stats to CSV.
    """
    #
    # From metadata - overview
    #
    insert_section(writer, "TOPIC CLASSIFICATION - FROM METADATA - OVERVIEW")

    insert_row(
        writer,
        "Total books with topic/subject info from metadata",
        (
            TopicClassification.select()
            .where(TopicClassification.from_metadata.is_null(False))
            .count()
        ),
    )

    insert_row(
        writer,
        "Total number of unique values for topic/subject assigned from metadata",
        (
            TopicClassification.select(TopicClassification.from_metadata)
            .where(TopicClassification.from_metadata.is_null(False))
            .distinct()
            .count()
        ),
    )

    #
    # From metadata - top 50
    #
    insert_section(writer, "TOPIC CLASSIFICATION - FROM METADATA - TOP 50")

    for i, item in enumerate(
        TopicClassification.select(
            TopicClassification.from_metadata,
            fn.COUNT(TopicClassification.from_metadata).alias("total"),
        )
        .where(TopicClassification.from_metadata.is_null(False))
        .group_by(TopicClassification.from_metadata)
        .order_by(fn.COUNT(TopicClassification.from_metadata).desc())
    ):

        if i >= 50:
            break

        insert_row(
            writer,
            f"Books labeled as {item.from_metadata}",
            item.total,
            item.from_metadata,
        )

    #
    # From detection - overview
    #
    insert_section(writer, "TOPIC CLASSIFICATION - FROM DETECTION - OVERVIEW")

    insert_row(
        writer,
        "Total books with topic/subject info from detection",
        (
            TopicClassification.select()
            .where(TopicClassification.from_detection.is_null(False))
            .count()
        ),
    )

    insert_row(
        writer,
        "Total number of unique values for topic/subject assigned from detection",
        (
            TopicClassification.select(TopicClassification.from_detection)
            .where(TopicClassification.from_detection.is_null(False))
            .distinct()
            .count()
        ),
    )

    insert_row(
        writer,
        "Average detection confidence score",
        (TopicClassification.select(fn.AVG(TopicClassification.detection_confidence)).scalar()),
    )

    #
    # From detection - breakdown
    #

    # Total books by topic
    insert_section(writer, "TOPIC CLASSIFICATION - FROM DETECTION - TOTAL BOOKS BY TOPIC")

    for i, item in enumerate(
        TopicClassification.select(
            TopicClassification.from_detection,
            fn.COUNT(TopicClassification.from_detection).alias("total"),
        )
        .where(TopicClassification.from_detection.is_null(False))
        .group_by(TopicClassification.from_detection)
        .order_by(TopicClassification.from_detection)
    ):

        insert_row(
            writer,
            f"Total books labeled as {item.from_detection}",
            item.total,
            item.from_detection,
        )

    # Average confidence by category
    insert_section(writer, "TOPIC CLASSIFICATION - FROM DETECTION - AVERAGE CONFIDENCE BY TOPIC")

    for i, item in enumerate(
        TopicClassification.select(
            TopicClassification.from_detection,
            fn.AVG(TopicClassification.detection_confidence).alias("average_confidence"),
        )
        .where(TopicClassification.from_detection.is_null(False))
        .group_by(TopicClassification.from_detection)
        .order_by(TopicClassification.from_detection)
    ):

        insert_row(
            writer,
            f"Average confidence for {item.from_detection}",
            item.average_confidence,
            item.from_detection,
        )

    #
    # Training set info
    #
    # Split between train, test and benchmark
    for set in ["train", "test", "benchmark"]:

        insert_section(
            writer,
            f"TOPIC CLASSIFICATION - FROM DETECTION - TRAINING SET - {set.upper()}",
        )

        # Total rows
        insert_row(
            writer,
            f"Total {set} rows in training set",
            (
                TopicClassificationTrainingDataset.select()
                .where(TopicClassificationTrainingDataset.set == set)
                .count()
            ),
        )

        # Total rows by target topic for that set
        for i, item in enumerate(
            TopicClassificationTrainingDataset.select(
                TopicClassificationTrainingDataset.target_topic,
                fn.COUNT(TopicClassificationTrainingDataset.target_topic).alias("total"),
            )
            .where(TopicClassificationTrainingDataset.set == set)
            .group_by(TopicClassificationTrainingDataset.target_topic)
            .order_by(TopicClassificationTrainingDataset.target_topic)
        ):

            insert_row(
                writer,
                f"Total {set} row for target topic {item.target_topic}",
                item.total,
                item.target_topic,
            )


def layout_aware_text_stats(writer: csv.writer):
    """
    Writes layout-aware text-related stats to CSV.
    """
    # Temporary
    insert_section(writer, f"LAYOUT-AWARE TEXT EXPORT")
    insert_row(writer, f"Total layout-aware texts exported", 0)
