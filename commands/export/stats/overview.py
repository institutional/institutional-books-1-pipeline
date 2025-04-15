import csv
from pathlib import Path

import click
import peewee

import utils
from models import (
    BookIO,
    PageCount,
    MainLanguage,
    TokenCount,
    HathitrustRightsDetermination,
    YearOfPublication,
    OCRQuality,
)
from const import OUTPUT_EXPORT_DIR_PATH, DATETIME_SLUG


@click.command("overview")
@utils.needs_pipeline_ready
def overview():
    """
    Generates a CSV with high-level statistics about the data collected by this pipeline.

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
        rights_determination_stats(writer)
        year_of_publication_stats(writer)
        ocr_quality_stats(writer)

        # OCR quality
        # Text-level language detection (> 10k tokens)
        # Text analysis metrics (+ tokenizability)
        # Deduplication data
        # Topic classification (+ split by book, + average confidence)
        # Layout-aware text stats

    click.echo(f"✅ {output_filepath.name} saved to disk.")


def books_stats(writer: csv.writer):  #
    writer.writerow(["BOOKS", " "])

    writer.writerow(
        [
            "Total books in collection",
            BookIO.select().count(),
        ]
    )

    writer.writerow(
        [
            "Total books with text",
            PageCount.select().where(PageCount.count_from_ocr > 0).count(),
        ]
    )

    writer.writerow(
        [
            "Total books without text",
            PageCount.select().where(PageCount.count_from_ocr < 1).count(),
        ]
    )


def token_count_stats(writer: csv.writer):
    writer.writerow(["TOKEN COUNTS", " "])

    target_llms_query = (
        TokenCount.select(TokenCount.target_llm).distinct().order_by(TokenCount.target_llm.desc())
    )

    for target_llm in [token_count.target_llm for token_count in target_llms_query]:
        writer.writerow(
            [
                f"{target_llm} - Total token count",
                TokenCount.select(peewee.fn.SUM(TokenCount.count))
                .where(TokenCount.target_llm == target_llm)
                .scalar(),
            ]
        )

        writer.writerow(
            [
                f"{target_llm} - Average token count",
                TokenCount.select(peewee.fn.AVG(TokenCount.count))
                .where(TokenCount.target_llm == target_llm)
                .scalar(),
            ]
        )


def page_count_stats(writer: csv.writer):
    writer.writerow(["PAGE COUNT", " "])

    writer.writerow(
        [
            "Total pages",
            PageCount.select(peewee.fn.SUM(PageCount.count_from_ocr)).scalar(),
        ]
    )

    writer.writerow(
        [
            "Average page count",
            PageCount.select(peewee.fn.AVG(PageCount.count_from_ocr)).scalar(),
        ]
    )


def main_language_stats(writer: csv.writer):
    writer.writerow(["BOOK-LEVEL MAIN LANGUAGE", " "])

    writer.writerow(
        [
            "Total unique main languages from metadata",
            MainLanguage.select(MainLanguage.from_metadata_iso693_2b)
            .where(
                (MainLanguage.from_metadata_iso693_2b.is_null(False))
                & (MainLanguage.from_metadata_iso693_2b != "und")
            )
            .distinct()
            .count(),
        ]
    )

    writer.writerow(
        [
            "Total unique main languages from detection",
            MainLanguage.select(MainLanguage.from_detection_iso693_3)
            .where(MainLanguage.from_detection_iso693_3.is_null(False))
            .distinct()
            .count(),
        ]
    )

    writer.writerow(
        [
            "Total books with no main language metadata",
            MainLanguage.select().where(MainLanguage.from_metadata_iso693_2b.is_null(True)).count(),
        ]
    )

    writer.writerow(
        [
            "Total books with no main language from detection",
            MainLanguage.select().where(MainLanguage.from_detection_iso693_3.is_null(True)).count(),
        ]
    )


def rights_determination_stats(writer: csv.writer):
    writer.writerow(["KNOWN RIGHTS STATUS", " "])

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
        writer.writerow(
            [
                f"Total books flagged as {rights_code.upper()} by Hathitrust",
                HathitrustRightsDetermination.select()
                .where(HathitrustRightsDetermination.rights_code == rights_code)
                .count(),
            ]
        )

    writer.writerow(
        [
            f"Total books with no rights code given by Hathitrust",
            HathitrustRightsDetermination.select()
            .where(HathitrustRightsDetermination.rights_code.is_null(True))
            .count(),
        ]
    )

    writer.writerow(
        [
            f'Total books flagged as either PD, PDUS or CC-ZERO and "Full view" by Hathitrust',
            HathitrustRightsDetermination.select()
            .where(
                (HathitrustRightsDetermination.rights_code in ["pd", "pdus", "cc-zero"])
                & (HathitrustRightsDetermination.us_rights_string == "Full view")
            )
            .count(),
        ]
    )

    writer.writerow(
        [
            f'Total books flagged as either PD, PDUS or CC-ZERO and "Full view" by Hathitrust that have text',
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
            .count(),
        ]
    )


def year_of_publication_stats(writer: csv.writer):
    writer.writerow(["PUBLICATION DATES", " "])

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
    for century in [entry.century for entry in centuries_query]:

        if century > 2000:
            continue

        writer.writerow(
            [
                f"(by century) Total books with a reported publication date in the {century}s",
                YearOfPublication.select().where(YearOfPublication.century == century).count(),
            ]
        )

    # Books by decade
    for decade in [entry.decade for entry in decades_query]:

        if decade > 2100:
            continue

        writer.writerow(
            [
                f"(by decade) Total books with a reported publication date in the {decade}s",
                YearOfPublication.select().where(YearOfPublication.decade == decade).count(),
            ]
        )

    # No date or invalid date
    writer.writerow(
        [
            f"Total books with no known or invalid publication date",
            YearOfPublication.select()
            .where(YearOfPublication.year.is_null(True) or YearOfPublication.year > 2100)
            .count(),
        ]
    )


def ocr_quality_stats(writer: csv.writer):
    writer.writerow(["OCR QUALITY", " "])

    # Average
    writer.writerow(
        [
            f"Google Books-provided - Average",
            OCRQuality.select(peewee.fn.AVG(OCRQuality.from_metadata)).scalar(),
        ]
    )

    writer.writerow(
        [
            f"pleias/OCRoscope - Average",
            OCRQuality.select(peewee.fn.AVG(OCRQuality.from_detection)).scalar(),
        ]
    )

    # No data
    writer.writerow(
        [
            f"Google Books-provided - Book with no score available",
            OCRQuality.select().where(OCRQuality.from_metadata.is_null(True)).count(),
        ]
    )

    writer.writerow(
        [
            f"pleias/OCRoscope - Book with no score available",
            OCRQuality.select().where(OCRQuality.from_detection.is_null(True)).count(),
        ]
    )

    # Average by decade
    decades_query = (
        YearOfPublication.select(YearOfPublication.decade)
        .where(YearOfPublication.decade.is_null(False))
        .distinct()
        .order_by(YearOfPublication.decade)
    )

    for decade in [entry.decade for entry in decades_query]:
        writer.writerow(
            [
                f"(by decade) Google Books-provided - Average for books likely published in the {decade}s",
                OCRQuality.select(peewee.fn.AVG(OCRQuality.from_metadata))
                .join(YearOfPublication, on=(OCRQuality.book == YearOfPublication.book))
                .where(YearOfPublication.decade == decade)
                .scalar(),
            ]
        )

    for decade in [entry.decade for entry in decades_query]:
        writer.writerow(
            [
                f"(by decade) pleias/OCRoscope - Average for books likely published in the {decade}s",
                OCRQuality.select(peewee.fn.AVG(OCRQuality.from_metadata))
                .join(YearOfPublication, on=(OCRQuality.book == YearOfPublication.book))
                .where(YearOfPublication.decade == decade)
                .scalar(),
            ]
        )
