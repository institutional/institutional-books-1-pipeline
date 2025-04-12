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

    _page_metadata_gzip = peewee.BlobField(
        null=True,
        column_name="page_metadata_gzip",
    )

    _words_gzip = peewee.BlobField(
        null=True,
        column_name="words_gzip",
    )

    _separators_gzip = peewee.BlobField(
        null=True,
        column_name="separators_gzip",
    )

    _spans_gzip = peewee.BlobField(
        null=True,
        column_name="spans_gzip",
    )

    spans_sorted = peewee.BooleanField(
        null=True,
        index=True,
    )

    _blocks_gzip = peewee.BlobField(
        null=True,
        column_name="blocks_gzip",
    )

    @property
    def page_metadata(self) -> list[PageMetadata]:
        """
        Handles automatic decompression and decoding for _page_metadata_gzip
        """
        return pickle.loads(gzip.decompress(self._text_by_page_gzip))

    @page_metadata.setter
    def page_metadata(self, pages: list[PageMetadata]):
        """
        Handles automatic encoding and compression for _page_metadata_gzip
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, PageMetadata)

        serialized = pickle.dumps(pages, protocol=5)
        self._page_metadata_gzip = gzip.compress(serialized)
        return self._page_metadata_gzip

    @property
    def words(self) -> list[list[OCRChunk]]:
        """
        Handles automatic decompression and decoding for _words_gzip
        """
        return pickle.loads(gzip.decompress(self._words_gzip))

    @words.setter
    def words(self, pages: list[list[OCRChunk]]):
        """
        Handles automatic encoding and compression for _words_gzip
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for word in page:
                assert isinstance(word, OCRChunk)
                assert word.type == OCRChunkType.WORD

        serialized = pickle.dumps(pages, protocol=5)
        self._words_gzip = gzip.compress(serialized)
        return self._words_gzip

    @property
    def separators(self) -> list[list[LayoutSeparator]]:
        """
        Handles automatic decompression and decoding for _separators_gzip
        """
        return pickle.loads(gzip.decompress(self._separators_gzip))

    @separators.setter
    def separators(self, pages: list[list[LayoutSeparator]]):
        """
        Handles automatic encoding and compression for _separators_gzip
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for separator in page:
                assert isinstance(separator, LayoutSeparator)

        serialized = pickle.dumps(pages, protocol=5)
        self._separators_gzip = gzip.compress(serialized)
        return self._separators_gzip

    @property
    def spans(self) -> list[list[OCRChunk]]:
        """
        Handles automatic decompression and decoding for _spans_gzip
        """
        return pickle.loads(gzip.decompress(self._spans_gzip))

    @spans.setter
    def spans(self, pages: list[list[OCRChunk]]):
        """
        Handles automatic encoding and compression for _spans_gzip
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for span in page:
                assert isinstance(span, OCRChunk)
                assert span.type == OCRChunkType.SPAN

        serialized = pickle.dumps(pages, protocol=5)
        self._spans_gzip = gzip.compress(serialized)
        return self._spans_gzip

    @property
    def blocks(self) -> list[list[OCRChunk]]:
        """
        Handles automatic decompression and decoding for _blocks_gzip
        """
        return pickle.loads(gzip.decompress(self._blocks_gzip))

    @spans.setter
    def blocks(self, pages: list[list[OCRChunk]]):
        """
        Handles automatic encoding and compression for _spans_gzip
        """
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for block in page:
                assert isinstance(block, OCRChunk)
                assert block.type == OCRChunkType.BLOCK

        serialized = pickle.dumps(pages, protocol=5)
        self._blocks_gzip = gzip.compress(serialized)
        return self._blocks_gzip
