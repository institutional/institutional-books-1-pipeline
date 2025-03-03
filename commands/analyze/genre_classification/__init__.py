import click

from .step01_extract_from_metadata import step01_extract_from_metadata


@click.group("genre-classification")
def genre_classification():
    """
    Command group: Genre classification.
    """
    pass


genre_classification.add_command(step01_extract_from_metadata)
