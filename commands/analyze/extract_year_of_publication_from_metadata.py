import re

import click

import utils
from models import BookIO, YearOfPublication


@click.command("extract-year-of-publication-from-metadata")
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
def extract_year_of_publication_from_metadata(
    overwrite: bool,
    start: int | None,
    end: int | None,
    db_write_batch_size: int,
):
    """
    Determines, for each record, the likely year of publication based on existing metadata.
    This is meant to be used for statistical analysis purposes only.

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        YearOfPublication.year,
        YearOfPublication.decade,
        YearOfPublication.century,
        YearOfPublication.source_field,
    ]

    for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():
        year_of_publication = None
        already_exists = False

        # Check if record already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            year_of_publication = YearOfPublication.get(book=book.barcode)
            assert year_of_publication
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} already analyzed.")
                continue
        except Exception:
            pass

        # Prepare record
        year_of_publication = YearOfPublication() if not already_exists else year_of_publication
        year_of_publication.book = book.barcode

        year, source_field = find_likely_publication_year(book)

        year_of_publication.year = year
        year_of_publication.decade = year // 10 * 10 if year is not None else None
        year_of_publication.century = year // 100 * 100 if year is not None else None
        year_of_publication.source_field = source_field

        if year:
            click.echo(f"🧮 #{book.barcode} was likely published in {year} ({source_field})")
        else:
            click.echo(f"🧮 #{book.barcode} - no info on publication date.")

        # Add to batch
        if already_exists:
            entries_to_update.append(year_of_publication)
        else:
            entries_to_create.append(year_of_publication)

        # Empty batches every X row
        if len(entries_to_create) + len(entries_to_update) >= db_write_batch_size:
            utils.process_db_write_batch(
                YearOfPublication,
                entries_to_create,
                entries_to_update,
                fields_to_update,
            )

    # Save remaining items from batches
    utils.process_db_write_batch(
        YearOfPublication,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )


def find_likely_publication_year(book: BookIO) -> tuple:
    """
    Analyses available metadata for a given book record and returns a tuple containing:
    - The year of publication
    - The field used to make that determination

    Returns a tuple containing (None, None) otherwise.
    """
    output = (None, None)

    year_regex = r"^[0-9]{4}$"

    fields_to_check = [
        "mods Publication Date",
        "gxml Date 1",
        "gxml Date 2",
    ]

    # Do not make an assessment if:
    # - "gxml Date Type" starts with "Continuing resource" (periodical, dates are for the whole journal)
    if str(book.csv_data["gxml Date Type"]).startswith("Continuing resource"):
        return output

    # - "gxml Date Type" contains "No attempt to code"
    if str(book.csv_data["gxml Date Type"]).startswith("No attempt to code"):
        return output

    for field in fields_to_check:
        year = book.csv_data[field]

        if year is None:
            continue

        if not year:
            continue

        if str(year) == "9999":
            continue

        if not re.match(year_regex, str(year)):
            continue

        year = int(year)

        output = (year, field)
        break

    return output
