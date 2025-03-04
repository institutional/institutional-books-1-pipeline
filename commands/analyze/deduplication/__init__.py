import click

from .step01_get_simhash import step01_get_simhash
from .step02_export_simhash_eval_sheet import step02_export_simhash_eval_sheet


@click.group("deduplication")
def deduplication():
    """
    Command group: Collection-level items deduplication
    """
    pass


deduplication.add_command(step01_get_simhash)
deduplication.add_command(step02_export_simhash_eval_sheet)
