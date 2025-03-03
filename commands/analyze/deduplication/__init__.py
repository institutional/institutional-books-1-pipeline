import click

from .step01_get_simhash import step01_get_simhash


@click.group("deduplication")
def deduplication():
    """
    Command group: Collection-level items deduplication
    """
    pass


deduplication.add_command(step01_get_simhash)
