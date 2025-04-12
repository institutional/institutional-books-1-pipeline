import gzip

import peewee
import orjson

from utils import get_db
from models import BookIO


class LayoutAwareText(peewee.Model):
    """
    `layout_aware_text` table: Stores layout-aware text generated from hOCR metadata for each book.
    """

    class Meta:
        table_name = "layout_aware_text"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    _text_by_page_gzip = peewee.BlobField(
        null=True,
        column_name="text_by_page_gzip",
    )

    @property
    def text_by_page(self) -> list[str]:
        """
        Handles automatic decompression and decoding for _text_by_page_gzip
        """
        return orjson.loads(gzip.decompress(self._text_by_page_gzip))

    @text_by_page.setter
    def text_by_page(self, text_by_page: list[str]):
        """
        Handles automatic encoding and compression for _text_by_page_gzip
        """
        assert isinstance(text_by_page, list)

        for page in text_by_page:
            assert isinstance(page, str)

        serialized = orjson.dumps(text_by_page)
        self._text_by_page_gzip = gzip.compress(serialized)
        return self._text_by_page_gzip
