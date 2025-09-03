import os
import re
import traceback
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import click
import requests
from bs4 import BeautifulSoup
from loguru import logger

from utils import (
    needs_pipeline_ready,
    needs_hathitrust_collection_prefix,
    get_batch_max_size,
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
    default=4,
    help="Determines how many subprocesses can be run in parallel. Be mindful of Hathitrust's resources!",
)
@needs_pipeline_ready
@needs_hathitrust_collection_prefix
def extract_hathitrust_rights_determination(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    max_workers: int,
):
    """
    Collects rights determination data from the Hathitrust API for this collection.

    Notes:
    - `--max-workers` defaults to 4.
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
            BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator(),
            start=1,
        ):
            batch.append(book)

            if len(batch) >= batch_max_size or i >= items_count:
                future = executor.submit(process_batch, batch, overwrite)
                futures.append(future)
                batch = []

        # Run batches in parallel
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.debug(traceback.format_exc())

                logger.error(
                    "Couldn't get rights determination data from Hathitrust. Interrupting."
                )

                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_batch(items: list[BookIO], overwrite=False) -> bool:
    """
    Processes a batch of BookIO entries.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        HathitrustRightsDetermination.from_record,
        HathitrustRightsDetermination.htid,
        HathitrustRightsDetermination.rights_code,
        HathitrustRightsDetermination.reason_code,
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

        htid = f"{os.getenv("HATHITRUST_COLLECTION_PREFIX")}.{book.barcode.lower()}"
        item_data = None  # data from the "items" field for that htid
        record_data = None  # data from the "records" field for that htid
        reason_code = None
        url = f"https://catalog.hathitrust.org/api/volumes/full/htid/{htid}.json"

        #
        # Check if database record already exists
        #
        try:
            item = HathitrustRightsDetermination.get(htid=htid)
            assert item
            already_exists = True

            if already_exists and not overwrite:
                logger.info(f"#{book.barcode} already analyzed")
                continue
        except:
            pass

        #
        # Pull data from Hathitrust
        #
        try:
            response = requests.get(url).json()
            assert response["records"]
            assert response["items"]
        except Exception as err:
            # logger.debug(traceback.format_exc())
            pass

        # Find data for this HTID in list of items
        try:
            item_data = None

            for ht_item in response["items"]:
                if ht_item["htid"].lower() == htid:
                    item_data = ht_item
                    break

            assert item_data
        except Exception as err:
            # logger.debug(traceback.format_exc())
            pass

        # Find data for this HTID in list of records (via fromRecord)
        try:
            record_data = response["records"][item_data["fromRecord"]]
            assert record_data
        except Exception as err:
            # logger.debug(traceback.format_exc())
            pass

        #
        # Try to grab reason code from MARC XML (via HT records section)
        #
        try:
            soup = BeautifulSoup(record_data["marc-xml"], "xml")

            # Go through all `<datafield tag="974">` entries
            for item in soup.find_all("datafield", {"tag": "974"}):
                subfields = {sub.get("code"): sub.text for sub in item.find_all("subfield")}

                # Only consider the one matching the current HTID (u field)
                if subfields["u"] != htid:
                    continue

                # Grab rights assessment reason (q field)
                reason_code = subfields["q"]

            assert reason_code
            assert re.match(r"^[a-z]+$", reason_code)
        except:
            # logger.debug(traceback.format_exc())
            pass

        #
        # Prepare record
        #
        item = HathitrustRightsDetermination() if not already_exists else item
        item.book = book.barcode
        item.htid = htid
        item.retrieved_date = datetime.now()

        if item_data:
            item.from_record = item_data["fromRecord"]
            item.rights_code = item_data["rightsCode"]
            item.reason_code = reason_code
            item.last_update_year = item_data["lastUpdate"][0:4]
            item.last_update_month = item_data["lastUpdate"][4:6]
            item.last_update_day = item_data["lastUpdate"][6:8]
            item.enumcron = item_data["enumcron"] if item_data["enumcron"] else None
            item.us_rights_string = item_data["usRightsString"]
            logger.info(f"#{book.barcode} -> {item.rights_code} ({item.reason_code})")
        else:
            logger.info(f"⏭️ #{book.barcode} -> No match.")

        if not already_exists:
            entries_to_create.append(item)
        else:
            entries_to_update.append(item)

    #
    # Save batches
    #
    process_db_write_batch(
        HathitrustRightsDetermination,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
