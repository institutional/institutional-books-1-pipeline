from .hf import hf

import click


@click.group("publish")
def publish():
    """Command group: Dataset publication."""
    pass


publish.add_command(hf)
