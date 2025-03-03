import click

import utils
from models import BookIO, TopicClassification


@click.command("step01-extract-from-metadata")
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
def step01_extract_from_metadata(
    overwrite: bool,
    start: int | None,
    end: int | None,
    db_write_batch_size: int,
):
    """
    Topic classification experiments, step 01:
    Collects the "topic/subject" classification of each book as expressed in the collection's metadata.

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []

    fields_to_update = [
        TopicClassification.from_metadata,
        TopicClassification.metadata_source,
    ]

    for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():
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
