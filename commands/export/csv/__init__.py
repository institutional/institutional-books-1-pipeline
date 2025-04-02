import click

from .overview import overview
from .table import table
from .deduplication_evaluation_sheet import deduplication_evaluation_sheet
from .simplified_source_metadata import simplified_source_metadata
from .topic_classification_training_dataset import topic_classification_training_dataset
from .random_ocr_text_chunks import random_ocr_text_chunks


@click.group("csv")
def csv():
    """Command group: Export of data as CSV spreadsheets."""
    pass


csv.add_command(overview)
csv.add_command(table)
csv.add_command(deduplication_evaluation_sheet)
csv.add_command(simplified_source_metadata)
csv.add_command(topic_classification_training_dataset)
csv.add_command(random_ocr_text_chunks)
