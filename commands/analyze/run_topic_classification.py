import csv
import traceback
from pathlib import Path
from datetime import datetime

import click
from slugify import slugify
from transformers import pipeline
from datasets import Dataset
import torch

import utils
from models import BookIO, TopicClassification, TopicClassificationTrainingDataset
from const import OUTPUT_EXPORT_DIR_PATH, DATETIME_SLUG

MODEL_NAME = "instdin/hlbooks-topic-classifier-bert-multilingual-uncased"


@click.command("run-topic-classification")
@click.option(
    "--benchmark-mode",
    is_flag=True,
    default=False,
    help="If set, runs in benchmark mode. Analyses 1000 benchmark samples and exports results as CSV",
)
@click.option(
    "--device",
    type=str,
    required=False,
    help="If set, allows to specify on which device the model should run. See utils.get_torch_devices().",
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
    "--db-write-batch-size",
    type=int,
    required=False,
    default=10_000,
    help="Determines the frequency at which records are pushed to the database. By default: once every 10,000 record creation/update request.",
)
@utils.needs_pipeline_ready
def run_topic_classification(
    benchmark_mode: bool,
    device: str | None,
    offset: int | None,
    limit: int | None,
    db_write_batch_size: int,
):
    """
    Runs a topic classification model on the collection.

    Notes:
    - The model was trained on the data filtered by `extract-topic-classification-training-dataset`
    - This command updates TopicClassification records

    Benchmark mode:
    - Runs topic classification model on 1000 records set aside for benchmarking purposes.
    - Results of the benchmark will be saved as:
      `/data/output/export/topic-classification-benchmark-{model-name}-{datetime}.csv`
    """
    #
    # Dependencies check
    #
    try:
        # Needs at least 1000 benchmark records
        assert (
            TopicClassificationTrainingDataset.select()
            .where(TopicClassificationTrainingDataset.set == "benchmark")
            .count()
            >= 1000
        )

        # Needs at least 5000 test/validation records
        assert (
            TopicClassificationTrainingDataset.select()
            .where(TopicClassificationTrainingDataset.set == "test")
            .count()
            >= 5000
        )

        # Needs at least 6000 train records
        assert (
            TopicClassificationTrainingDataset.select()
            .where(TopicClassificationTrainingDataset.set == "train")
            .count()
            >= 6000
        )

        # Existing records for each book in TopicClassification
        assert BookIO.select().count() == TopicClassification.select().count()
    except Exception:
        click.echo(
            "Topic classification dataset is not ready. "
            + "See `extract-topic-classification-training-dataset`."
        )
        exit(1)

    #
    # Isolated benchmark mode
    #
    if benchmark_mode:
        run_benchmark(device)
        exit(0)

    #
    # Full processing mode
    #
    run_on_collection(device, offset, limit, db_write_batch_size)


def run_on_collection(
    device: str | None,
    offset: int | None,
    limit: int | None,
    db_write_batch_size: int,
):
    """
    Run classfication model against collection, update TopicClassification records.
    """
    pipe = pipeline("text-classification", model=MODEL_NAME, device=device)

    items_count = TopicClassification.select().offset(offset).limit(limit).count()
    items_to_update = []

    fields_to_update = [
        TopicClassification.from_detection,
        TopicClassification.detection_confidence,
        TopicClassification.detection_source,
    ]

    for i, item in enumerate(
        TopicClassification.select()
        .offset(offset)
        .limit(limit)
        .order_by(TopicClassification.book)
        .iterator(),
        start=1,
    ):

        # Run detection on item
        try:
            prompt = utils.get_metadata_as_text_prompt(item.book, skip_topic=True, skip_genre=True)
            result = pipe(prompt)

            item.from_detection = result[0]["label"]
            item.detection_confidence = result[0]["score"]
            item.detection_source = MODEL_NAME

            items_to_update.append(item)

            click.echo(f"🧮 #{item.book.barcode} = {item.from_detection} from {item.from_metadata}")
        except Exception:
            click.echo(traceback.format_exc())
            click.echo(f"⏭️ Could not run classifier on #{item.book.barcode}. Skipping.")

        # Update database records in batches
        if len(items_to_update) >= db_write_batch_size or i >= items_count:
            utils.process_db_write_batch(
                TopicClassification,
                [],
                items_to_update,
                fields_to_update,
            )


def run_benchmark(device: str | None):
    """
    Runs the classification model against records set aside for benchmarking purposes.
    Yields benchmark scores and a summary sheet.
    """
    click.echo("🥼 Running topic classification task in benchmark mode.")
    click.echo(f"🧪 Target model: {MODEL_NAME}.")

    pipe = pipeline("text-classification", model=MODEL_NAME, device=device)

    rows = []

    row_template = {
        "barcode": "",
        "prompt": "",
        "target_topic": "",
        "detected_topic": "",
        "confidence": "",
        "match": "",
        "model_name": "",
    }

    total_valid = 0
    total_invalid = 0

    start = datetime.now()
    end = None

    #
    # Evaluate
    #
    for item in (
        TopicClassificationTrainingDataset.select()
        .where(TopicClassificationTrainingDataset.set == "benchmark")
        .iterator()
    ):
        book = item.book
        row = dict(row_template)
        row["barcode"] = book.barcode
        row["prompt"] = utils.get_metadata_as_text_prompt(book, skip_topic=True, skip_genre=True)
        row["target_topic"] = item.target_topic
        row["model_name"] = MODEL_NAME

        try:
            result = pipe(row["prompt"])
            row["detected_topic"] = result[0]["label"]
            row["confidence"] = result[0]["score"]
        except:
            pass

        if row["detected_topic"] == row["target_topic"]:
            row["match"] = "YES"
            total_valid += 1
        else:
            row["match"] = "NO"
            total_invalid += 1

        rows.append(row)

    end = datetime.now()

    #
    # Write to CSV
    #
    output_filepath = Path(
        OUTPUT_EXPORT_DIR_PATH,
        f"topic-classification-benchmark-{slugify(MODEL_NAME)}-{DATETIME_SLUG}.csv",
    )

    with open(output_filepath, "w+") as fd:
        writer = csv.writer(fd)
        writer.writerow(list(row_template.keys()))
        [writer.writerow(list(row.values())) for row in rows]

    click.echo(f"✅ {output_filepath.name} saved to disk.")

    #
    # Print stats
    #
    click.echo(f"🧮 Total samples: {len(rows)}")
    click.echo(f"✅ {total_valid} benchmark matches")
    click.echo(f"⛔ {total_invalid} benchmark mistmatches")
    click.echo(f"⏱️ Evaluation runtime: {end - start}")
