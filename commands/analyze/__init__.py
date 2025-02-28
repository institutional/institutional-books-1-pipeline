import click

from .token_count import token_count
from .page_count import page_count
from .year_of_publication import year_of_publication


@click.group("analyze")
def analyze():
    """Command group: Corpus analysis."""
    pass


analyze.add_command(token_count)
analyze.add_command(page_count)
analyze.add_command(year_of_publication)
