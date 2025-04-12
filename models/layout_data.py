import gzip
import pickle
from dataclasses import dataclass
from enum import Enum

import peewee

from utils import get_db
from models import BookIO


@dataclass(repr=True)
class PageMetadata:
    width: int = 0
    height: int = 0
    number: int = 0
    language: str = "unknown"
    complex_layout: bool = False


class OCRChunkType(Enum):
    UNKNOWN = 1
    WORD = 2
    SPAN = 3
    BLOCK = 4


@dataclass(repr=True)
class OCRChunk:
    x_min: int = -1
    y_min: int = -1
    x_max: int = -1
    y_max: int = -1
    text: str = ""
    confidence: int = 0
    type: OCRChunkType = OCRChunkType.UNKNOWN
    font_size: int = -1


class LayoutSeparatorType(Enum):
    UNKNOWN = 0
    HORIZONTAL = 1
    VERTICAL = 2


@dataclass(repr=True)
class LayoutSeparator:
    x_min: int = -1
    y_min: int = -1
    x_max: int = -1
    y_max: int = -1
    type: LayoutSeparatorType = LayoutSeparatorType.UNKNOWN


class LayoutData(peewee.Model):
    """
    `layout_data` table: Stores per-book layout data extracted from hOCR.
    """

    class Meta:
        table_name = "layout_data"
        database = get_db()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
        primary_key=True,
    )

    _page_metadata_by_page_gzip = peewee.BlobField(
        null=True,
        column_name="page_metadata_by_page_gzip",
    )

    _words_by_page_gzip = peewee.BlobField(
        null=True,
        column_name="words_by_page_gzip",
    )

    _separators_by_page_gzip = peewee.BlobField(
        null=True,
        column_name="separators_by_page_gzip",
    )

    _spans_by_page_gzip = peewee.BlobField(
        null=True,
        column_name="spans_by_page_gzip",
    )

    spans_sorted = peewee.BooleanField(
        null=True,
        index=True,
    )

    _blocks_by_page_gzip = peewee.BlobField(
        null=True,
        column_name="blocks_by_page_gzip",
    )

    @property
    def page_metadata_by_page(self) -> list[PageMetadata]:
        """
        Handles automatic decompression and decoding for the underlying property.
        """
        return pickle.loads(gzip.decompress(self._page_metadata_by_page_gzip))

    @page_metadata_by_page.setter
    def page_metadata_by_page(self, pages: list[PageMetadata]):
        """
        Handles automatic encoding and compression for the underlying property.
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, PageMetadata)

        serialized = pickle.dumps(pages, protocol=5)
        self._page_metadata_by_page_gzip = gzip.compress(serialized)
        return self._page_metadata_by_page_gzip

    @property
    def words_by_page(self) -> list[list[OCRChunk]]:
        """
        Handles automatic decompression and decoding for the underlying property.
        """
        return pickle.loads(gzip.decompress(self._words_by_page_gzip))

    @words_by_page.setter
    def words_by_page(self, pages: list[list[OCRChunk]]):
        """
        Handles automatic encoding and compression for the underlying property.
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for word in page:
                assert isinstance(word, OCRChunk)
                assert word.type == OCRChunkType.WORD

        serialized = pickle.dumps(pages, protocol=5)
        self._words_by_page_gzip = gzip.compress(serialized)
        return self._words_by_page_gzip

    @property
    def separators_by_page(self) -> list[list[LayoutSeparator]]:
        """
        Handles automatic decompression and decoding for the underlying property.
        """
        return pickle.loads(gzip.decompress(self._separators_by_page_gzip))

    @separators_by_page.setter
    def separators_by_page(self, pages: list[list[LayoutSeparator]]):
        """
        Handles automatic encoding and compression for the underlying property.
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for separator in page:
                assert isinstance(separator, LayoutSeparator)

        serialized = pickle.dumps(pages, protocol=5)
        self._separators_by_page_gzip = gzip.compress(serialized)
        return self._separators_by_page_gzip

    @property
    def spans_by_page(self) -> list[list[OCRChunk]]:
        """
        Handles automatic decompression and decoding for the underlying property.
        """
        return pickle.loads(gzip.decompress(self._spans_by_page_gzip))

    @spans_by_page.setter
    def spans_by_page(self, pages: list[list[OCRChunk]]):
        """
        Handles automatic encoding and compression for the underlying property.
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for span in page:
                assert isinstance(span, OCRChunk)
                assert span.type == OCRChunkType.SPAN

        serialized = pickle.dumps(pages, protocol=5)
        self._spans_by_page_gzip = gzip.compress(serialized)
        return self._spans_by_page_gzip

    @property
    def blocks_by_page(self) -> list[list[OCRChunk]]:
        """
        Handles automatic decompression and decoding for the underlying property.
        """
        return pickle.loads(gzip.decompress(self._blocks_by_page_gzip))

    @blocks_by_page.setter
    def blocks_by_page(self, pages: list[list[OCRChunk]]):
        """
        Handles automatic encoding and compression for the underlying property.
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for block in page:
                assert isinstance(block, OCRChunk)
                assert block.type == OCRChunkType.BLOCK

        serialized = pickle.dumps(pages, protocol=5)
        self._blocks_by_page_gzip = gzip.compress(serialized)
        return self._blocks_by_page_gzip
