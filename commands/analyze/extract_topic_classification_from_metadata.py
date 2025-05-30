import click

import utils
from models import BookIO, TopicClassification


@click.command("extract-topic-classification-from-metadata")
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
def extract_topic_classification_from_metadata(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    db_write_batch_size: int,
):
    """
    Collects topic/subject classification data for each book from the collection's metadata.

    Notes:
    - Extracted from `gxml Subject Added Entry-Topical Term` (via `book.csv_data`).
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        TopicClassification.from_metadata,
        TopicClassification.metadata_source,
    ]

    for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
        topic_classification = None
        already_exists = False

        # Check if record already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            topic_classification = TopicClassification.get(book=book.barcode)
            assert topic_classification
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} already analyzed.")
                continue
        except Exception:
            pass

        # Prepare record
        topic_classification = TopicClassification() if not already_exists else topic_classification
        topic_classification.book = book.barcode
        topic_classification.metadata_source = "gxml Subject Added Entry-Topical Term"

        from_metadata = book.csv_data["gxml Subject Added Entry-Topical Term"]

        if from_metadata.strip():
            topic_classification.from_metadata = from_metadata
            click.echo(f"🧮 #{book.barcode} = {from_metadata} (metadata)")
        else:
            click.echo(f"🧮 #{book.barcode} - no valid topic/subject info.")

        # Add to batch
        if already_exists:
            entries_to_update.append(topic_classification)
        else:
            entries_to_create.append(topic_classification)

        # Empty batches every X row
        if len(entries_to_create) + len(entries_to_update) >= db_write_batch_size:
            utils.process_db_write_batch(
                TopicClassification,
                entries_to_create,
                entries_to_update,
                fields_to_update,
            )

    # Save remaining items from batches
    utils.process_db_write_batch(
        TopicClassification,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
