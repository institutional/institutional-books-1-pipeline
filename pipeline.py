""" Entry point: Initializes app context and groups commands. """

import click

from dotenv import load_dotenv

import utils
import commands.setup
import commands.analyze

load_dotenv()
utils.check_env()
utils.make_dirs()


@click.group()
def cli():
    pass


cli.add_command(commands.setup.setup)
cli.add_command(commands.analyze.analyze)

if __name__ == "__main__":
    cli()
