import click

from .build import build
from .clear import clear
from .status import status


@click.group("setup")
def setup():
    """Command group: Pipeline setup."""
    pass


setup.add_command(build)
setup.add_command(clear)
setup.add_command(status)
