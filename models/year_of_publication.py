import peewee

from utils import get_db

from models import BookIO


class YearOfPublication(peewee.Model):
    """
    `year_of_publication` table: Keeps track of probable year of publication for each record, based on existing metadata.
    """

    class Meta:
        table_name = "year_of_publication"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    year = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    source_field = peewee.CharField(
        max_length=128,
        unique=False,
        index=True,
    )
    """ Metadata field used as reference. """
