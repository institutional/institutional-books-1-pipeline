from pathlib import Path
import pickle

import click
from model2vec.distill import distill
from model2vec.train import StaticModelForClassification
from slugify import slugify

import utils
from models import OCRPostprocessingTrainingDataset
from models.ocr_postprocessing_training_dataset import TARGET_TYPES
from const import DATETIME_SLUG, OUTPUT_MODELS_DIR_PATH


@click.command("step02-train-and-evaluate-model")
@click.option(
    "--source-model-name",
    type=str,
    required=False,
    default="sentence-transformers/LaBSE",
    help="Name of the text similarity model to distill and fine-tune with Model2Vec.",
)
@utils.needs_pipeline_ready
def step02_train_and_evaluate_model(source_model_name: str):
    """
    TODO
    """
    model_filepath = Path(
        OUTPUT_MODELS_DIR_PATH,
        f"{slugify(source_model_name.split("/")[1])}-ocr-postprocessing-{DATETIME_SLUG}.pickle",
    )

    train_split_texts = []
    train_split_labels = []

    test_split_texts = []
    test_split_labels = []

    distilled = None
    classifier = None

    #
    # Prepare training dataset
    #
    click.echo(f"🏋️ Compiling training set ...")

    for entry in (
        OCRPostprocessingTrainingDataset.select()
        .where(OCRPostprocessingTrainingDataset.target_type.is_null(False))
        .iterator()
    ):

        training_repr = entry.get_training_repr()
        target_type = TARGET_TYPES.index(entry.target_type)

        if entry.set == "train":
            train_split_texts.append(training_repr)
            train_split_labels.append(target_type)

        if entry.set == "test":
            test_split_texts.append(training_repr)
            test_split_labels.append(target_type)

    click.echo(f"- Train split: {len(train_split_texts)} entries.")
    click.echo(f"- Test split: {len(test_split_texts)} entries.")

    #
    # Distill model
    #
    click.echo(f"⚗️ Distilling {source_model_name} with Model2Vec ...")
    distilled = distill(source_model_name)

    #
    # Train classifier
    #
    click.echo(f"🏋️ Fine-tuning {source_model_name} distill as a classifier ...")
    classifier = StaticModelForClassification.from_static_model(model=distilled)

    classifier = classifier.fit(
        train_split_texts,
        train_split_labels,
        learning_rate=0.0015,
        max_epochs=4,
    )
    # learning_rate=0.0015, max_epochs=3

    #
    # Evaluating classifier
    #
    click.echo(f"🏋️ Evaluating classifier ...")

    classification_report = classifier.evaluate(
        test_split_texts,
        test_split_labels,
    )

    click.echo(classification_report)

    #
    # Save classifier
    #
    with open(model_filepath, "wb+") as fd:
        pickle.dump(classifier, fd)
    click.echo(f"✅ {model_filepath.name} saved to disk.")
