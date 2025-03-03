import peewee

from utils import get_db

from models import BookIO


class ScannedTextSimhash(peewee.Model):
    """
    `scanned_text_simhash` table: Records a simhash for every scanned text in the collection.
    """

    class Meta:
        table_name = "scanned_text_simhash"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    hash = peewee.CharField(
        max_length=128,
        null=True,
        unique=False,
        index=True,
    )
