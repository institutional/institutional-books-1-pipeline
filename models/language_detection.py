import peewee

from utils import get_db

from models import BookIO


class LanguageDetection(peewee.Model):
    """
    `language_detection` table: Keep track of the token count for one of the detected languages for a given book.
    """

    class Meta:
        table_name = "language_detection"
        database = get_db()

    id_language_detection = peewee.PrimaryKeyField()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
    )

    iso639_2b = peewee.CharField(
        max_length=3,
        null=True,
        unique=False,
        index=True,
    )

    iso639_3 = peewee.CharField(
        max_length=3,
        null=True,
        unique=False,
        index=True,
    )

    token_count = peewee.IntegerField(
        null=True,
        unique=False,
    )

    tokenizer = peewee.CharField(
        max_length=512,
        null=True,
        unique=False,
        index=True,
    )

    percentage_of_total = peewee.FloatField(
        null=True,
        unique=False,
    )

    detection_source = peewee.CharField(
        max_length=256,
        null=True,
        unique=False,
        index=True,
    )
