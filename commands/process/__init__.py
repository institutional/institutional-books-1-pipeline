import click

from .generate_layout_aware_text import generate_layout_aware_text


@click.group("process")
def process():
    """Command group: Corpus processing / augmentation."""
    pass


process.add_command(generate_layout_aware_text)
