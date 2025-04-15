import csv
from pathlib import Path

import click
import peewee

import utils
from models import BookIO, PageCount, MainLanguage, TokenCount, HathitrustRightsDetermination
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

        #
        # Text-level language detection
        #

        #
        # Text analysis metrics
        #

        #
        # Deduplication data
        #

    click.echo(f"✅ {output_filepath.name} saved to disk.")


def books_stats(writer: csv.writer):
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
