import os
import gzip
import pickle
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from models import BookIO
from const import OUTPUT_MISC_DIR_PATH

LAYOUT_DATA_DIR_PATH = Path(OUTPUT_MISC_DIR_PATH, "layout-data")
""" Path to the folder containing layout data objects. """


@dataclass(repr=True)
class PageMetadata:
    """
    Page-level metadata, extracted from hOCR files.
    """

    width: int = 0
    height: int = 0
    number: int = 0
    language: str = "unknown"
    complex_layout: bool = False


class OCRChunkType(Enum):
    """
    Possible types for OCRChunk.
    """

    UNKNOWN = 1
    WORD = 2
    SPAN = 3
    BLOCK = 4


@dataclass(repr=True)
class OCRChunk:
    """
    Fragment-level metadata, either directly extracted from hOCR files or reprocessed.
    """

    x_min: int = -1
    y_min: int = -1
    x_max: int = -1
    y_max: int = -1
    text: str = ""
    confidence: int = 0
    type: OCRChunkType = OCRChunkType.UNKNOWN


class LayoutSeparatorType(Enum):
    """
    Possible types of layout separators.
    """

    UNKNOWN = 0
    HORIZONTAL = 1
    VERTICAL = 2


@dataclass(repr=True)
class LayoutSeparator:
    """
    Detected layout separators. Extracted from source images.
    """

    x_min: int = -1
    y_min: int = -1
    x_max: int = -1
    y_max: int = -1
    type: LayoutSeparatorType = LayoutSeparatorType.UNKNOWN


@dataclass(repr=True)
class LayoutData:
    """
    Holds extracted and processed layout data from a book's row data.
    A single instance contains data for all pages of a given book

    Notes:
    - Is used to generate layout aware text exports.
    - Underlying file is stored on disk as a gzipped pickle.
    """

    _barcode: str = ""
    _page_metadata_by_page: list[PageMetadata] = None
    _words_by_page: list[list[OCRChunk]] = None
    _separators_by_page: list[list[LayoutSeparator]] = None
    _spans_by_page: list[list[OCRChunk]] = None
    _spans_sorted: bool = False
    _blocks_by_page: list[list[OCRChunk]] = None

    @classmethod
    def exists(cls, barcode) -> bool:
        """
        Checks if the underlying file for the storage of a given barcode exists.
        """
        path = LayoutData.get_filepath(barcode)
        return path.exists()

    @classmethod
    def delete(cls, barcode) -> bool:
        """
        Deletes the underlying storage file for a given barcode.
        """
        filepath = LayoutData.get_filepath(barcode)

        if filepath.exists(barcode):
            filepath.unlink()

        return True

    @classmethod
    def get(cls, barcode):
        """
        Attempts to load a LayoutData object from storage for a given barcode
        """
        if not LayoutData.exists(barcode):
            raise FileNotFoundError(f"{barcode}")

        raw_data = None

        with open(LayoutData.get_filepath(barcode), "rb+") as fd:
            raw_data = fd.read()

        return pickle.loads(gzip.decompress(raw_data))

    def save(self) -> bool:
        """
        Serializes (pickle) and compress (gzip) the current object before storing it to disk.
        """
        filepath = LayoutData.get_filepath(self.barcode)

        with open(filepath, "wb+") as fd:
            serialized = pickle.dumps(self, protocol=5)
            fd.write(gzip.compress(serialized))

        return True

    @classmethod
    def get_filepath(cls, barcode) -> Path:
        """
        Returns the filepath for saving layout data for a given barcode.
        """
        os.makedirs(LAYOUT_DATA_DIR_PATH, exist_ok=True)
        return Path(LAYOUT_DATA_DIR_PATH, f"{barcode}.pickle.gz")

    def get_book(self) -> BookIO:
        """
        Shortcut: returns BookIO record associated with this layout data.
        """
        return BookIO.get(barcode=self.barcode)

    @property
    def barcode(self) -> str:
        return self._barcode

    @barcode.setter
    def barcode(self, barcode: str) -> str:
        assert isinstance(barcode, str)
        self._barcode = barcode
        return self._barcode

    @property
    def page_metadata_by_page(self) -> list[PageMetadata]:
        return self._page_metadata_by_page

    @page_metadata_by_page.setter
    def page_metadata_by_page(self, pages: list[PageMetadata]):
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, PageMetadata)

        self._page_metadata_by_page = pages
        return self._page_metadata_by_page

    @property
    def words_by_page(self) -> list[list[OCRChunk]]:
        return self._words_by_page

    @words_by_page.setter
    def words_by_page(self, pages: list[list[OCRChunk]]):
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for word in page:
                assert isinstance(word, OCRChunk)
                assert word.type == OCRChunkType.WORD

        self._words_by_page = pages
        return self._words_by_page

    @property
    def separators_by_page(self) -> list[list[LayoutSeparator]]:
        return self._separators_by_page

    @separators_by_page.setter
    def separators_by_page(self, pages: list[list[LayoutSeparator]]):
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for separator in page:
                assert isinstance(separator, LayoutSeparator)

        self._separators_by_page = pages
        return self._separators_by_page

    @property
    def spans_by_page(self) -> list[list[OCRChunk]]:
        return self._spans_by_page

    @spans_by_page.setter
    def spans_by_page(self, pages: list[list[OCRChunk]]):
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for span in page:
                assert isinstance(span, OCRChunk)
                assert span.type == OCRChunkType.SPAN

        self._spans_by_page = pages
        return self._spans_by_page

    @property
    def blocks_by_page(self) -> list[list[OCRChunk]]:
        return self._blocks_by_page

    @blocks_by_page.setter
    def blocks_by_page(self, pages: list[list[OCRChunk]]):
        assert isinstance(pages, list)

        for page in pages:
            assert isinstance(page, list)

            for block in page:
                assert isinstance(block, OCRChunk)
                assert block.type == OCRChunkType.BLOCK

        self._blocks_by_page = pages
        return self._blocks_by_page

    @property
    def spans_sorted(self) -> bool:
        return self._spans_sorted

    @spans_sorted.setter
    def spans_sorted(self, value: bool) -> bool:
        self._spans_sorted = bool(value)
        return self._spans_sorted


def merge_ocrchunks(
    chunks_to_merge: list[OCRChunk],
    new_type: OCRChunkType = OCRChunkType.SPAN,
) -> OCRChunk:
    """
    Merges a list of OCRChunk objects.
    """
    x_min = min(chunk.x_min for chunk in chunks_to_merge)
    y_min = min(chunk.y_min for chunk in chunks_to_merge)
    x_max = max(chunk.x_max for chunk in chunks_to_merge)
    y_max = max(chunk.y_max for chunk in chunks_to_merge)

    text = "".join(chunk.text for chunk in chunks_to_merge)

    confidence = int(sum(chunk.confidence for chunk in chunks_to_merge) / len(chunks_to_merge))

    return OCRChunk(
        x_min=x_min,
        y_min=y_min,
        x_max=x_max,
        y_max=y_max,
        text=text,
        confidence=confidence,
        type=new_type,
    )
