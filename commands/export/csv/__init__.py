import click

from .overview import overview
from .table import table


@click.group("csv")
def csv():
    """Command group: Export of data as CSV spreadsheets."""
    pass


csv.add_command(overview)
csv.add_command(table)
