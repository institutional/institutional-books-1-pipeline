import traceback
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import click
import requests

from utils import (
    needs_pipeline_ready,
    get_batch_max_size,
    get_simhash_features,
    process_db_write_batch,
)
from models import BookIO, HathitrustRightsDetermination


@click.command("extract-hathitrust-rights-determination")
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
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--limit",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=8,
    help="Determines how many subprocesses can be run in parallel. Be mindful of Hathitrust's resources!",
)
@needs_pipeline_ready
def extract_hathitrust_rights_determination(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    max_workers: int,
):
    """
    Attempts to match Harvard Library's Google Books records with Hathitrust's Rights Determination records.

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    # Create batches of books to process
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        batch = []

        items_count = BookIO.select().offset(offset).limit(limit).count()

        batch_max_size = get_batch_max_size(
            items_count=items_count,
            max_workers=max_workers,
        )

        # Create batches of items to process
        for i, book in enumerate(
            BookIO.select().offset(offset).limit(limit).order_by(BookIO.book).iterator(),
            start=1,
        ):
            batch.append(book)

            if len(batch) >= batch_max_size or i >= items_count:
                future = executor.submit(process_batch, batch, overwrite)
                futures.append(future)
                batch = []

        # Run them in parallel processes, update records as they come back
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())

                click.echo(
                    "Could not pull rights determination data from Hathitrust. Interrupting."
                )

                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_batch(items: list[BookIO], overwrite=False) -> bool:
    """
    Processes a batch of BookIO etnries.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        HathitrustRightsDetermination.from_record,
        HathitrustRightsDetermination.htid,
        HathitrustRightsDetermination.rights_code,
        HathitrustRightsDetermination.last_update_year,
        HathitrustRightsDetermination.last_update_month,
        HathitrustRightsDetermination.last_update_day,
        HathitrustRightsDetermination.enumcron,
        HathitrustRightsDetermination.us_rights_string,
        HathitrustRightsDetermination.retrieved_date,
    ]

    for book in items:
        item = None
        already_exists = False

        htid = f"hvd.{book.barcode.lower()}"
        ht_data = None
        url = f"https://catalog.hathitrust.org/api/volumes/full/htid/{htid}.json"

        # Check if record already exists
        try:
            item = HathitrustRightsDetermination.get(htid=htid)
            assert item
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} already analyzed.")
                continue
        except:
            pass

        # Pull data from Hathitrust
        try:
            # Pull data
            response = requests.get(url).json()
            assert response.items

            # Find specific record in list of items
            ht_data = None

            for record in response.items:
                if record.htid.lower() == htid:
                    ht_data = record
                    break

            assert ht_data
        except:
            pass

        # Skip if no match found
        if not ht_data:
            click.echo(f"⏭️ {book.barcode} -> No match. Skipping.")
            continue

        # Prepare record
        item = HathitrustRightsDetermination() if not already_exists else item
        item.book.barcode = book.barcode
        item.from_record = ht_data["fromRecord"]
        item.htid = htid
        item.rights_code = ht_data["rightsCode"]
        item.last_update_year = ht_data["lastUpdate"][0:4]
        item.last_update_month = ht_data["lastUpdate"][4:6]
        item.last_update_day = ht_data["lastUpdate"][6:8]
        item.enumcron = ht_data["enumcron"]
        item.us_rights_string = ht_data["usRightsString"]
        item.retrieved_date = datetime.now()

        click.echo(f"{book.barcode} -> {item.rights_code}")

        if already_exists:
            entries_to_update.append(item)
        else:
            entries_to_update.append(item)

    # Save batches
    process_db_write_batch(
        HathitrustRightsDetermination,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
