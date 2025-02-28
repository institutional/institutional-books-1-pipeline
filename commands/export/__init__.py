import click

from .csv import csv
from .viz import viz


@click.group("export")
def export():
    """Command group: Export of data and pre-computed stats/graphs."""
    pass


export.add_command(csv)
export.add_command(viz)
