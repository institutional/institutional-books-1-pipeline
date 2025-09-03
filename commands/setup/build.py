import csv
from datetime import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
from loguru import logger


import utils
import models
from models import BookIO
import utils.pipeline_readiness


@click.command("build")
@click.option(
    "--skip-indexing",
    is_flag=True,
    default=False,
    help="If set, will skip indexing records from books_latest.csv.",
)
@click.option(
    "--skip-caching",
    is_flag=True,
    default=False,
    help="If set, will skip caching text.",
)
@click.option(
    "--cache-offset",
    type=int,
    required=False,
    help="If set, allows for caching a subset of the collection's OCR tex.",
)
@click.option(
    "--cache-limit",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the collection (sorted by BookIO.barcode).",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=4,
    help="Determines how many volumes can be retrieved in parallel.",
)
def build(
    skip_indexing: bool,
    skip_caching: bool,
    cache_offset: int,
    cache_limit: int,
    max_workers: int,
):
    """
    Initializes the pipeline:
    - Sets up the local database
    - Pulls information about the collection from cloud storage (output of GRIN Transfer)
    - Indexes individual volumes as `BookIO` records
    - Caches text from individual volume on disk

    Notes:
    - Can be run every time remote storage is updated. Updates existing records.
    - Update runs do not delete volumes that may have disapeared from `books_latest.csv` (unlikely)
    """
    #
    # Database setup
    #
    logger.info("Setting up the database (if needed) ...")

    with utils.get_db() as db:
        try:
            available_models = [model_name for model_name in dir(models) if model_name[0].isupper()]
            db.create_tables(
                [models.__getattribute__(model_name) for model_name in available_models]
            )
        except Exception:
            logger.debug(traceback.format_exc())
            logger.error("Could not initialize database.")
            exit(1)

    #
    # Indexing (+ mark pipeline as ready)
    #
    if not skip_indexing:
        logger.info("Indexing collection from `books_latest.csv` ...")

        try:
            index_collection()
            utils.pipeline_readiness.set_pipeline_readiness(True)
        except Exception:
            logger.debug(traceback.format_exc())
            logger.error("Error while indexing collection. Interrupting.")
            exit(1)

    #
    # Caching
    #
    if not skip_caching:
        logger.info("Caching OCR text ...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            batch = []
            items_count = BookIO.select().offset(cache_offset).limit(cache_limit).count()

            batch_max_size = utils.get_batch_max_size(
                items_count=items_count,
                max_workers=max_workers,
            )

            # Create batches of items to process
            for i, book in enumerate(
                BookIO.select()
                .offset(cache_offset)
                .limit(cache_limit)
                .order_by(BookIO.barcode)
                .iterator(),
                start=1,
            ):
                batch.append(book)

                if len(batch) >= batch_max_size or i >= items_count:
                    future = executor.submit(cache_books_batch, batch)
                    futures.append(future)
                    batch = []

            # Run batches in parallel
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    logger.debug(traceback.format_exc())
                    logger.error("Error while refreshing OCR text cache. Interrupting.")
                    executor.shutdown(wait=False, cancel_futures=True)
                    exit(1)


def index_collection() -> bool:
    """
    Create or update `BookIO` records from remote `books_latest.csv`.
    """
    entries_to_create = []
    entries_to_update = []

    with BookIO.get_collection_csv(ignore_cache=True) as csv_file:

        csv_line = csv_file.readline().decode("utf-8").rstrip("\n")
        headers = csv.reader([csv_line]).__next__()

        while True:
            csv_offset = csv_file.tell()
            csv_line = csv_file.readline().decode("utf-8").rstrip("\n")

            if not csv_line:  # EOF
                break

            metadata = csv.DictReader([csv_line], headers).__next__()
            barcode = metadata["Barcode"]
            archive_is_available = False
            metadata_is_enriched = False

            try:
                assert isinstance(
                    datetime.fromisoformat(metadata["Sync Timestamp"]),
                    datetime,
                )
                archive_is_available = True
            except:
                pass

            try:
                assert isinstance(
                    datetime.fromisoformat(metadata["Enrichment Timestamp"]),
                    datetime,
                )
                metadata_is_enriched = True
            except:
                pass

            # Update record if it exists, create it otherwise
            try:
                entry = BookIO.get(barcode=barcode)
                entry.metadata_csv_offset = csv_offset
                entry.archive_is_available = archive_is_available
                entry.metadata_is_enriched = metadata_is_enriched
                entries_to_update.append(entry)

                logger.info(f"#{barcode} BookIO record was updated")
            except:
                entry = BookIO(
                    barcode=barcode,
                    metadata_csv_offset=csv_offset,
                    archive_is_available=archive_is_available,
                    metadata_is_enriched=metadata_is_enriched,
                )
                entries_to_create.append(entry)

                logger.info(f"#{barcode} BookIO record was created")

    logger.info(f"Updating database with new BookIO records ...")

    utils.process_db_write_batch(
        model=BookIO,
        entries_to_create=entries_to_create,
        entries_to_update=entries_to_update,
        fields_to_update=[BookIO.metadata_csv_offset],
    )

    return True


def cache_books_batch(books: list[BookIO]) -> bool:
    """
    Accesses the text of a given volume so it can be cached on disk.
    """
    for book in books:
        text_by_page = book.text_by_page
        logger.info(f"#{book.barcode}'s OCR text has been cached ({len(text_by_page)} pages)")

    return True
