import click

from .step01_extract_from_metadata import step01_extract_from_metadata


@click.group("topic-classification")
def topic_classification():
    """
    Command group: Topic classification.
    """
    pass


topic_classification.add_command(step01_extract_from_metadata)
