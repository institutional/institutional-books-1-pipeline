import peewee

from utils import get_db

from models import BookIO


class PageCount(peewee.Model):
    """
    `page_count` table: Stores page count for a given record.
    """

    class Meta:
        table_name = "page_count"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    count_from_ocr = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )
    """ Total pages in OCR'd text. """
