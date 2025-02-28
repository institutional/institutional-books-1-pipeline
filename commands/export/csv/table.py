import click

import utils


@click.command("table")
@utils.needs_pipeline_ready
def table():
    """ """
    click.echo("TODO")
