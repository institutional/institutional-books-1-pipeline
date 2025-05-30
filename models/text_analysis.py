import peewee

from utils import get_db

from models import BookIO


class TextAnalysis(peewee.Model):
    """
    `text_analysis` table: Keeps track of text analysis metrics on OCR'd texts.
    """

    class Meta:
        table_name = "text_analysis"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    char_count = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )

    char_count_continous = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    word_count = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    word_count_unique = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )

    word_type_token_ratio = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )

    bigram_count = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )

    bigram_count_unique = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )

    bigram_type_token_ratio = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )

    trigram_count = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )

    trigram_count_unique = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )

    trigram_type_token_ratio = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )

    sentence_count = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    sentence_count_unique = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )

    sentence_type_token_ratio = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )

    sentence_average_length = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )

    tokenizability_o200k_base_ratio = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )
