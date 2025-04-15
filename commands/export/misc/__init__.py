import click

from .deduplication_evaluation_sheet import deduplication_evaluation_sheet
from .simplified_source_metadata import simplified_source_metadata
from .topic_classification_training_dataset import topic_classification_training_dataset


@click.group("misc")
def misc():
    """Command group: Misc."""
    pass


misc.add_command(deduplication_evaluation_sheet)
misc.add_command(simplified_source_metadata)
misc.add_command(topic_classification_training_dataset)
