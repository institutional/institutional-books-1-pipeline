import csv
import traceback
from pathlib import Path
from datetime import datetime

import click
from slugify import slugify
from transformers import pipeline

import utils
from models import BookIO, TopicClassificationTrainingDataset
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
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
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
    overwrite: bool,
    db_write_batch_size: int,
):
    """
    WIP
    """
    #
    # Dependencies check
    #
    try:
        assert (
            TopicClassificationTrainingDataset.select()
            .where(TopicClassificationTrainingDataset.set == "benchmark")
            .count()
            >= 1000
        )
        assert (
            TopicClassificationTrainingDataset.select()
            .where(TopicClassificationTrainingDataset.set == "test")
            .count()
            >= 5000
        )
        assert (
            TopicClassificationTrainingDataset.select()
            .where(TopicClassificationTrainingDataset.set == "train")
            .count()
        )
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
        run_benchmark()
        exit(0)

    click.echo("Not implemented")


def run_benchmark():
    """ """
    click.echo("🥼 Running topic classification task in benchmark mode.")
    click.echo(f"🧪 Target model: {MODEL_NAME}.")

    pipe = pipeline("text-classification", model=MODEL_NAME)

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
