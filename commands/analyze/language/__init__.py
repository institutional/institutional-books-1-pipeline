import click

from .step01_extract_from_metadata import step01_extract_from_metadata
from .step02_detect import step02_detect


@click.group("language")
def language():
    """
    Command group: Language content analysis.
    """
    pass


language.add_command(step01_extract_from_metadata)
language.add_command(step02_detect)
