import csv
from pathlib import Path

import click
from loguru import logger

import utils
from models import TopicClassificationTrainingDataset
from const import EXPORT_DIR_PATH, DATETIME_SLUG


@click.command("topic-classification-training-dataset")
@utils.needs_pipeline_ready
def topic_classification_training_dataset():
    """
    Exports the topic classification training dataset prepared via `analyze extract-topic-classification-training-dataset` as a series of CSVs.

    Current setup: text classification fine-tunning
    https://huggingface.co/docs/autotrain/en/text_classification

    Saved as:
    - `/data/output/export/topic-classification-training-dataset-{set}-{datetime}.csv`
    """
    for set in ["train", "test", "benchmark"]:
        output_filepath = Path(
            EXPORT_DIR_PATH,
            f"topic-classification-training-dataset-{set}-{DATETIME_SLUG}.csv",
        )

        with open(output_filepath, "w+") as fd:
            writer = csv.writer(fd)
            writer.writerow(["text", "target"])

            for entry in (
                TopicClassificationTrainingDataset.select()
                .where(TopicClassificationTrainingDataset.set == set)
                .iterator()
            ):
                text = utils.get_metadata_as_text_prompt(
                    entry.book,
                    skip_topic=True,
                    skip_genre=True,
                )
                target = entry.target_topic

                writer.writerow([text, target])

            logger.info(f"{output_filepath.name} saved to disk")
