import click

from .layout_aware_text import layout_aware_text


@click.group("process")
def process():
    """Command group: Corpus processing / augmentation."""
    pass


process.add_command(layout_aware_text)
