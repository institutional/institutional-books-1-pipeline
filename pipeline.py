"""Entry point: Initializes app context and groups commands."""

import sys

import click
from dotenv import load_dotenv
from loguru import logger

import utils
from commands.setup import setup as cmd_setup
from commands.analyze import analyze as cmd_analyze
from commands.process import process as cmd_process
from commands.export import export as cmd_export
from commands.publish import publish as cmd_publish

load_dotenv()
utils.check_env()
utils.make_dirs()


@click.group()
@click.option(
    "--verbose",
    is_flag=True,
    help="If set, includes DEBUG-level statements in log output.",
)
def cli(verbose: bool):
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stderr, level=level)


cli.add_command(cmd_setup)
cli.add_command(cmd_analyze)
cli.add_command(cmd_process)
cli.add_command(cmd_export)
cli.add_command(cmd_publish)

if __name__ == "__main__":
    cli()
