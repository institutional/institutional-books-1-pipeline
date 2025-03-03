import click

from .step01_extract_from_metadata import step01_extract_from_metadata


@click.group("language")
def topic_classification():
    """
    Command group: Language content analysis.
    """
    pass


topic_classification.add_command(step01_extract_from_metadata)
