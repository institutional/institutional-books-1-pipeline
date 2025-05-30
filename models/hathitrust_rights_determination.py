from datetime import datetime

import peewee

from utils import get_db

from models import BookIO


class HathitrustRightsDetermination(peewee.Model):
    """
    `hathitrust_rights_determination` table:
    Keeps track of BookIO matches with Hathitrust's rights determination records.

    Example of source data:
    - https://catalog.hathitrust.org/api/volumes/full/htid/hvd.hn6kfn.json
    """

    class Meta:
        table_name = "hathitrust_rights_determination"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    from_record = peewee.CharField(
        max_length=128,
        null=True,
        unique=False,
        index=True,
    )

    htid = peewee.CharField(
        max_length=128,
        null=True,
        unique=True,
        index=True,
    )

    rights_code = peewee.CharField(
        max_length=32,
        null=True,
        unique=False,
        index=True,
    )
    # https://www.hathitrust.org/the-collection/preservation/rights-database/#:~:text=for%20open%20access-,Attributes,-Attributes

    reason_code = peewee.CharField(
        max_length=32,
        null=True,
        unique=False,
        index=True,
    )
    # https://www.hathitrust.org/the-collection/preservation/rights-database/#:~:text=note%20for%20details-,Reasons,-Rights%20code%20reasons

    last_update_year = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    last_update_month = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    last_update_day = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    enumcron = peewee.CharField(
        max_length=64,
        null=True,
        unique=False,
        index=True,
    )

    us_rights_string = peewee.CharField(
        max_length=64,
        null=True,
        unique=False,
        index=True,
    )

    retrieved_date = peewee.DateTimeField(
        default=datetime.now,
    )
