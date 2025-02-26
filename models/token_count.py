import peewee

from utils import get_db

from models import BookIO


class TokenCount(peewee.Model):
    """
    `token_count` table: Stores token counts for a given record.
    """

    class Meta:
        table_name = "token_count"
        database = get_db()

    token_count_id = peewee.PrimaryKeyField()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
    )

    target_llm = peewee.CharField(
        max_length=512,
        null=True,
        unique=False,
        index=True,
    )

    tokenizer = peewee.CharField(
        max_length=512,
        null=True,
        unique=False,
        index=True,
    )

    count = peewee.IntegerField(
        null=True,
        unique=False,
        index=False,
    )
