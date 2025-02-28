import click

import utils


@click.command("overview")
@utils.needs_pipeline_ready
def overview():
    """ """
    click.echo("TODO")
