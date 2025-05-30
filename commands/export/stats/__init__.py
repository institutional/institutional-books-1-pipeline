import click

from .overview import overview


@click.group("stats")
def stats():
    """Command group: Export of precomputed stats and visualizations."""
    pass


stats.add_command(overview)
