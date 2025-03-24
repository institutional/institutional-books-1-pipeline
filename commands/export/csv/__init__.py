import click

from .overview import overview
from .table import table
from .deduplication_evaluation_sheet import deduplication_evaluation_sheet


@click.group("csv")
def csv():
    """Command group: Export of data as CSV spreadsheets."""
    pass


csv.add_command(overview)
csv.add_command(table)
csv.add_command(deduplication_evaluation_sheet)
