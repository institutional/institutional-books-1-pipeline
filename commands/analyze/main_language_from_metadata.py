import re

import click
import iso639

import utils
from models import BookIO, MainLanguage


@click.command("main-language-from-metadata")
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
    "--db-write-batch-size",
    type=int,
    required=False,
    default=10_000,
    help="Determines the frequency at which records are pushed to the database. By default: once every 10,000 record creation/update request.",
)
@utils.needs_pipeline_ready
def main_language_from_metadata(
    overwrite: bool,
    start: int | None,
    end: int | None,
    db_write_batch_size: int,
):
    """
    Collects the main language of each book as expressed in the collection's metadata.

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        MainLanguage.from_metadata_iso693_2b,
        MainLanguage.from_metadata_iso693_3,
        MainLanguage.metadata_source,
    ]

    for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():
        main_language = None
        already_exists = False

        # Check if record already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            main_language = MainLanguage.get(book=book.barcode)
            assert main_language
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} already analyzed.")
                continue
        except Exception:
            pass

        # Prepare record
        main_language = MainLanguage() if not already_exists else main_language
        main_language.book = book.barcode
        main_language.metadata_source = "gxml_language"

        try:
            gxml_language = iso639.Lang(pt2b=book.csv_data["gxml Language"])
            main_language.from_metadata_iso693_2b = gxml_language.pt2b
            main_language.from_metadata_iso693_3 = gxml_language.pt3
            click.echo(f"🧮 #{book.barcode} = {gxml_language.pt3} (metadata)")
        except Exception:
            click.echo(f"🧮 #{book.barcode} - no valid language info.")

        # Add to batch
        if already_exists:
            entries_to_update.append(main_language)
        else:
            entries_to_create.append(main_language)

        # Empty batches every X row
        if len(entries_to_create) + len(entries_to_update) >= db_write_batch_size:
            utils.process_db_write_batch(
                MainLanguage,
                entries_to_create,
                entries_to_update,
                fields_to_update,
            )

    # Save remaining items from batches
    utils.process_db_write_batch(
        MainLanguage,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
