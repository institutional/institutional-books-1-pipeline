from .step01_generate_training_dataset import step01_generate_training_dataset
from .step02_train_and_evaluate_model import step02_train_and_evaluate_model
from .step03_process import step03_process

import click

from models.ocr_postprocessing_training_dataset import TARGET_TYPES


@click.group("ocr-postprocessing")
def ocr_postprocessing():
    """Group of commands related to OCR postprocessing"""
    pass


ocr_postprocessing.add_command(step01_generate_training_dataset)
ocr_postprocessing.add_command(step02_train_and_evaluate_model)
ocr_postprocessing.add_command(step03_process)
