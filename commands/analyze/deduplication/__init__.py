import multiprocessing
from statistics import median
from collections import Counter

import click
import peewee

from .step01_get_simhash import step01_get_simhash
from .step02_export_simhash_eval_sheet import step02_export_simhash_eval_sheet


@click.group("deduplication")
def deduplication():
    """
    Command group: Collection-level items deduplication
    """
    pass


deduplication.add_command(step01_get_simhash)
deduplication.add_command(step02_export_simhash_eval_sheet)


def get_filtered_duplicates() -> dict:
    """
    TODO: WIP
    """
    from models import BookIO, ScannedTextSimhash, MainLanguage

    hashes_to_books = {}

    #
    # Group all books by simhash into a hashmap
    #
    for entry in (
        ScannedTextSimhash.select()
        .where(ScannedTextSimhash.hash.is_null(False))
        .group_by(ScannedTextSimhash.hash)
        .having(peewee.fn.COUNT(ScannedTextSimhash.hash) > 1)
        .iterator()
    ):
        hash = entry.hash
        book = entry.book

        if hash is None:
            continue

        if hashes_to_books.get(hash, None) is None:
            hashes_to_books[hash] = []

        hashes_to_books[hash].append(book)

    #
    # For each group, eliminate likely false positives
    #
    # TODO

    # Detected language is an outlier

    # Continuous character count < or > 33% average

    return hashes_to_books
