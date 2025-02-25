import os
import re
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import click

import utils
import models
from models import BookIO
from const import TABLES


@click.command("build")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
)
@click.option(
    "--tables-only",
    is_flag=True,
    default=False,
    help="If set, will only create tables and stop.",
)
def build(overwrite: bool, tables_only: bool):
    """ """
    click.echo("BUILD")
