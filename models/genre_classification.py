import peewee

from utils import get_db

from models import BookIO


class GenreClassification(peewee.Model):
    """
    `genre_classification` table: Keeps track of genre classification data for each book from different sources.
    """

    class Meta:
        table_name = "genre_classification"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    from_metadata = peewee.CharField(
        max_length=256,
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

    from_detection = peewee.CharField(
        max_length=256,
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

    detection_average_linear_logprob = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )

    detection_perplexity = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )
