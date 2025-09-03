import click
from loguru import logger

import utils
from models import BookIO, GenreClassification


@click.command("extract-genre-classifciation-from-metadata")
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
def extract_genre_classification_from_metadata(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    db_write_batch_size: int,
):
    """
    Collects genre/form classification data for each book from the collection's metadata.

    Notes:
    - Extracted from `MARC Genres` (via `book.metadata`).
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        GenreClassification.from_metadata,
        GenreClassification.metadata_source,
    ]

    for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
        genre_classification = None
        already_exists = False

        # Check if record already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            genre_classification = GenreClassification.get(book=book.barcode)
            assert genre_classification
            already_exists = True

            if already_exists and not overwrite:
                logger.info(f"#{book.barcode} already analyzed")
                continue
        except Exception:
            pass

        # Prepare record
        genre_classification = GenreClassification() if not already_exists else genre_classification
        genre_classification.book = book.barcode
        genre_classification.metadata_source = "MARC Genres"

        from_metadata = book.metadata["MARC Genres"]

        if from_metadata.strip():
            genre_classification.from_metadata = from_metadata
            logger.info(f"#{book.barcode} = {from_metadata} (metadata)")
        else:
            logger.warning(f"#{book.barcode} - no valid genre/form info")

        # Add to batch
        if already_exists:
            entries_to_update.append(genre_classification)
        else:
            entries_to_create.append(genre_classification)

        # Empty batches every X row
        if len(entries_to_create) + len(entries_to_update) >= db_write_batch_size:
            utils.process_db_write_batch(
                GenreClassification,
                entries_to_create,
                entries_to_update,
                fields_to_update,
            )

    # Save remaining items from batches
    utils.process_db_write_batch(
        GenreClassification,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
