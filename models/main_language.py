import peewee

from utils import get_db

from models import BookIO


class MainLanguage(peewee.Model):
    """
    `main_language` table: Keeps track of most likely main language for each book.
    """

    class Meta:
        table_name = "main_language"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    from_metadata_iso639_2b = peewee.CharField(
        max_length=3,
        null=True,
        unique=False,
        index=True,
    )

    from_metadata_iso639_3 = peewee.CharField(
        max_length=3,
        null=True,
        unique=False,
        index=True,
    )

    metadata_source = peewee.CharField(
        max_length=256,
        null=True,
        unique=False,
        index=True,
    )

    from_detection_iso639_2b = peewee.CharField(
        max_length=3,
        null=True,
        unique=False,
        index=True,
    )

    from_detection_iso639_3 = peewee.CharField(
        max_length=3,
        null=True,
        unique=False,
        index=True,
    )

    detection_source = peewee.CharField(
        max_length=256,
        null=True,
        unique=False,
        index=True,
    )
