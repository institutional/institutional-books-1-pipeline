import click

from .step01_extract_from_metadata import step01_extract_from_metadata


@click.group("language")
def language():
    """
    Command group: Language content analysis.
    """
    pass


language.add_command(step01_extract_from_metadata)
