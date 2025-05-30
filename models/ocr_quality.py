import peewee

from utils import get_db

from models import BookIO


class OCRQuality(peewee.Model):
    """
    `ocr_quality` table: Keeps track of OCR quality data for each book from different sources.
    """

    class Meta:
        table_name = "ocr_quality"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    from_metadata = peewee.IntegerField(
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

    from_detection = peewee.IntegerField(
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
