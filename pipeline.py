"""Entry point: Initializes app context and groups commands."""

import click

from dotenv import load_dotenv

import utils
import commands.setup
import commands.analyze
import commands.process
import commands.export

load_dotenv()
utils.check_env()
utils.make_dirs()


@click.group()
def cli():
    pass


cli.add_command(commands.setup.setup)
cli.add_command(commands.analyze.analyze)
cli.add_command(commands.process.process)
cli.add_command(commands.export.export)


if __name__ == "__main__":
    cli()
