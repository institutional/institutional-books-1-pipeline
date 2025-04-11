import os
import json
import csv
from pathlib import Path
import tarfile
from dataclasses import dataclass
from io import BytesIO

import peewee

from utils import get_db, get_cache, get_s3_client
from const import (
    INPUT_JSONL_DIR_PATH,
    INPUT_CSV_DIR_PATH,
    GRIN_TO_S3_TRANCHES_TO_BUCKET_NAMES,
    GRIN_TO_S3_BUCKET_VERSION_PREFIX,
)


@dataclass(repr=True)
class BookRawData:
    """
    Holds data parsed from a .tar.gz file containg a book's raw data (scans, OCR metadata ...)
    """

    images: list[bytes]
    """ Raw bytes for all the images available for the selected book, indexed by page order. (.jp2 or .tif files) """

    hocr: list[str]
    """ Strings for all hOCR files available for the selected book, indexed by page order. (hOCR as .html files)"""

    text: list[str]
    """ Strings for all raw text files available for the selected book, indexed by page order. (.txt files)"""

    gxml: str
    """ String for the main GXML file available for teh selected book. (.xml file) """

    md5: list[tuple[str, str]]
    """ Data parsed from checksum.md5. """


class BookIO(peewee.Model):
    """
    `book_io` table: Organizes information about each JSONL entry (individual books).
    Stores offset information to allow for random access.
    """

    def __init__(self, *args, **kwargs):
        super(BookIO, self).__init__(*args, **kwargs)
        self.__jsonl_data = None
        self.__csv_data = None
        self.__raw_data = None

    __book_csv_headers = None
    """ Class-level cache for the headers of "books.csv"."""

    class Meta:
        table_name = "book_io"
        database = get_db()

    barcode = peewee.CharField(
        max_length=64,
        null=False,
        unique=True,
        index=True,
        primary_key=True,
    )

    tranche = peewee.CharField(
        max_length=32,
        unique=False,
        index=True,
    )
    """ Current tranche (Google Books Viewability status) for that book. """

    jsonl_file_number = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )
    """ If filename is VIEW_FULL-0214.jsonl, this number is 214. """

    jsonl_offset = peewee.BigIntegerField(
        null=True,
        unique=False,
        index=False,
    )
    """ Used to position file cursor in JSONL file. Marks beginning of entry within JSONL file. """

    csv_offset = peewee.BigIntegerField(
        null=True,
        unique=False,
        index=False,
    )
    """ Used to position file cursor in CSV file. Marks beginning of entry within CSV file. """

    @property
    def jsonl_data(self):
        """
        Getter for __jsonl_data.
        Automatically loads data from appropriate JSONL if available
        """
        if (
            not self.__jsonl_data
            and self.jsonl_file_number is not None
            and self.jsonl_offset is not None
        ):
            jsonl_filepath = os.path.join(
                INPUT_JSONL_DIR_PATH,
                f"{self.tranche}-{self.jsonl_file_number:04}.jsonl",
            )

            with open(jsonl_filepath, "r+") as jsonl_file:
                jsonl_file.seek(self.jsonl_offset)

                line = jsonl_file.readline()
                self.__jsonl_data = json.loads(line)

        return self.__jsonl_data

    @jsonl_data.setter
    def jsonl_data(self, value):
        self.__jsonl_data = value

    @property
    def csv_data(self):
        """
        Getter for __csv_data.
        Automatically loads data from appropriate CSV if available
        """
        csv_filepath = Path(INPUT_CSV_DIR_PATH, f"{self.tranche}-books.csv")

        # Load __book_csv_headers cache if not set
        # NOTE: This assumes every single CSV in the collection has the same set of headers.
        # We know that to be true, but is a pretty important assumption.
        if not BookIO.__book_csv_headers:
            with open(csv_filepath, "r+") as csv_file:
                headers = csv_file.readline()
                BookIO.__book_csv_headers = csv.reader([headers]).__next__()

        if not self.__csv_data and self.csv_offset:
            with open(csv_filepath, "r+") as csv_file:
                csv_file.seek(self.csv_offset)
                csv_line = csv_file.readline()

                csv_data = csv.DictReader([csv_line], BookIO.__book_csv_headers).__next__()
                self.__csv_data = csv_data

        return self.__csv_data

    @csv_data.setter
    def csv_data(self, value):
        self.__csv_data = value

    @property
    def text(self) -> str:
        """
        Returns the full OCR'd text of the current book
        """
        return self.jsonl_data["text_by_page"]

    @property
    def merged_text(self) -> str:
        """
        Returns the full OCR'd text of the current book merged as a single string
        """
        return "\n".join(self.jsonl_data["text_by_page"])

    @property
    def tarball(self) -> bytes:
        """
        Retrieves the tarball containg raw data for the current book.
        Tries to load it from cache if available.
        """
        book_tgz_name = f"{self.barcode}.tar.gz"
        book_tgz_bytes = None

        #
        # Look for tarball in cache
        #
        with get_cache() as cache:
            book_tgz_bytes = cache.get(book_tgz_name, None)

            if book_tgz_bytes:
                return book_tgz_bytes

        #
        # Load tarball from R2 otherwise
        #

        # Determine bucket based on tranche info
        bucket_name = GRIN_TO_S3_TRANCHES_TO_BUCKET_NAMES[self.tranche]
        bucket_name = bucket_name.replace("gbooks-", "gbooks-raw-")  # This is "raw" bucket

        # Load object from R2
        try:
            book_tgz_bytes = get_s3_client().get_object(
                Key=f"{GRIN_TO_S3_BUCKET_VERSION_PREFIX}/{book_tgz_name}",
                Bucket=bucket_name,
            )
            assert book_tgz_bytes

            book_tgz_bytes = book_tgz_bytes["Body"].read()
            assert book_tgz_bytes
        except Exception as err:
            raise FileNotFoundError(f"Could not find raw data for barcode {self.barcode}") from err

        # Save copy in cache
        with get_cache() as cache:
            cache.set(book_tgz_name, book_tgz_bytes)

        return book_tgz_bytes

    @property
    def raw_data(self) -> BookRawData:
        """
        Parses the tarball containing raw data for the current book and returns a BookRawData object.
        """
        # If available, return copy already present in memory
        if self.__raw_data:
            return self.raw_data

        book_tgz_bytes = self.tarball
        images = []
        hocr = []
        text = []
        gxml = []
        md5 = []

        with tarfile.open(fileobj=BytesIO(book_tgz_bytes), mode="r:gz") as tar:
            sorted_members = sorted(tar.getmembers(), key=lambda m: m.name)

            for member in sorted_members:

                if ".tif" in member.name or ".jp2" in member.name:
                    data = tar.extractfile(member.name).read()
                    images.append(data)

                if ".html" in member.name:
                    data = tar.extractfile(member.name).read().decode("utf-8")
                    hocr.append(data)

                if ".txt" in member.name:
                    data = tar.extractfile(member.name).read().decode("utf-8")
                    text.append(data)

                if ".xml" in member.name:
                    gxml = tar.extractfile(member.name).read().decode("utf-8")

                # Example of md5 line:
                # 802d3e8fbb3d659fcf1fa2d298bd80bc  00000004.tif
                if ".md5" in member.name:
                    data = tar.extractfile(member.name).read().decode("utf-8")

                    for line in data.split("\n"):
                        if not line:
                            continue

                        md5.append(tuple(line.split("  ")))

        assert len(text) == len(hocr) == len(images)
        assert len(md5) == (len(text) + len(hocr) + len(images)) + 1
        assert gxml

        self.__raw_data = BookRawData(
            images=images,
            hocr=hocr,
            text=text,
            gxml=gxml,
            md5=md5,
        )

        return self.__raw_data
