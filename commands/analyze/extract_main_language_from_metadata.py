import click
import iso639

import utils
from models import BookIO, MainLanguage


@click.command("extract-main-language-from-metadata")
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
    "--db-write-batch-size",
    type=int,
    required=False,
    default=10_000,
    help="Determines the frequency at which the database will be updated (every X entries). By default: every 10,000 entries.",
)
@utils.needs_pipeline_ready
def extract_main_language_from_metadata(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    db_write_batch_size: int,
):
    """
    Collects book-level language data for each book from the collection's metadata.

    Notes:
    - Extracted from `gxml Language` (via `book.csv_data`).
    - Original data is in ISO 639-2B format. This command stores it both in this format as well as ISO 639-3.
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        MainLanguage.from_metadata_iso639_2b,
        MainLanguage.from_metadata_iso639_3,
        MainLanguage.metadata_source,
    ]

    for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
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
        main_language.metadata_source = "gxml Language"

        try:
            gxml_language = iso639.Lang(pt2b=book.csv_data["gxml Language"])
            main_language.from_metadata_iso639_2b = gxml_language.pt2b
            main_language.from_metadata_iso639_3 = gxml_language.pt3
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
