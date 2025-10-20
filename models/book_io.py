import os
import json
import csv
from pathlib import Path
import tarfile
from dataclasses import dataclass
from io import BytesIO, TextIOWrapper
import gzip

import peewee
import orjson
from loguru import logger

from utils import get_db, get_cache, get_s3_client
from const import OCR_POSTPROCESSING_DIR_PATH


@dataclass(repr=True)
class BookTarballData:
    """
    Holds data parsed from a Google Books tarball containg a volume's raw data (scans, OCR metadata ...)
    """

    images: list[bytes]
    """ Raw bytes for all the images available for the selected book, indexed by page order. (.jp2 or .tif files) """

    hocr: list[str]
    """ Strings for all hOCR files available for the selected book, indexed by page order. (hOCR as .html files)"""

    text: list[str]
    """ Strings for all raw text files available for the selected book, indexed by page order. (.txt files)"""

    gxml: str
    """ String for the main GXML file available for teh selected book. (Google-flavored METS XML file) """

    md5: list[tuple[str, str]]
    """ Data parsed from checksum.md5. """


class BookIO(peewee.Model):
    """
    `book_io` table: Organizes information about each volume present in the collection.
    """

    def __init__(self, *args, **kwargs):
        super(BookIO, self).__init__(*args, **kwargs)
        self.__text_by_page = None
        self.__metadata = None
        self.__parsed_tarball = None
        self.__postprocessed_ocr = None

    __book_csv_headers = None
    """ In-memory, class-level cache for the headers of "books.csv"."""

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

    metadata_csv_offset = peewee.BigIntegerField(
        null=True,
        unique=False,
        index=False,
    )
    """ Used to position file cursor within books_latest.csv. Marks beginning of entry within CSV file. """

    archive_is_available = peewee.BooleanField(
        null=True,
        unique=False,
        index=True,
    )

    metadata_is_enriched = peewee.BooleanField(
        null=True,
        unique=False,
        index=True,
    )

    @property
    def text_by_page(self) -> list[str]:
        """
        Getter for `__text_by_page`: OCR'd test for the current volume.
        Underlying JSONL file will be loaded from remote storage or disk cache.
        """
        if self.archive_is_available == False:
            return []

        if self.__text_by_page is not None:
            return self.__text_by_page

        jsonl_bytes = None

        run_name = os.getenv("GRIN_DATA_RUN_NAME")
        bucket_name = os.getenv("GRIN_DATA_FULL_BUCKET")
        is_gz = os.getenv("GRIN_DATA_FULL_IS_COMPRESSED", "1") == "1"
        filename = f"{self.barcode}_ocr.jsonl" if not is_gz else f"{self.barcode}_ocr.jsonl.gz"

        cache_key = f"{bucket_name}:{run_name}/{filename}"

        #
        # Try to load JSONL from cache
        #
        with get_cache() as cache:
            jsonl_bytes = cache.get(cache_key, None)

        #
        # Load it from storage otherwise
        #
        if jsonl_bytes is None:
            try:
                response = get_s3_client().get_object(
                    Key=f"{run_name}/{filename}",
                    Bucket=bucket_name,
                )
                assert response

                jsonl_bytes = response["Body"].read()

                if is_gz:
                    jsonl_bytes = gzip.decompress(jsonl_bytes)

                assert jsonl_bytes
            except Exception as err:
                raise FileNotFoundError(f"Could not retrieve {filename}") from err

            # Save parsed copy in cache
            with get_cache() as cache:
                cache.set(cache_key, jsonl_bytes)

        # Parse jsonl_bytes
        self.__text_by_page = []

        for json_line in jsonl_bytes.decode("utf-8").split("\n"):
            if not json_line:
                continue

            self.__text_by_page.append(json.loads(json_line))

        return self.__text_by_page

    @text_by_page.setter
    def text_by_page(self, value):
        self.__text_by_page = value

    @property
    def merged_text(self) -> str:
        """
        Returns the full OCR'd text of the current book merged as a single string
        """
        return "\n".join(self.text_by_page)

    @property
    def metadata(self) -> dict:
        """
        Getter for `__metadata`: volume metadata from `books_latest.csv`.
        Fetches a specific row from `books_latest.csv` based on `self.metadata_csv_offset`.
        Automatically loads `books_latest.csv` (either from storage or disk cache).
        """
        # Populate `__book_csv_headers` memory cache if not set
        if not BookIO.__book_csv_headers:
            with BookIO.get_collection_csv() as csv_file:
                headers = csv_file.readline().decode("utf-8").rstrip("\n")
                BookIO.__book_csv_headers = csv.reader([headers]).__next__()

        # Retrieve targeted row
        if not self.__metadata and self.metadata_csv_offset:
            with BookIO.get_collection_csv() as csv_file:
                csv_file.seek(self.metadata_csv_offset)
                csv_line = csv_file.readline().decode("utf-8").rstrip("\n")

                metadata = csv.DictReader([csv_line], BookIO.__book_csv_headers).__next__()
                self.__metadata = metadata

        return self.__metadata

    @metadata.setter
    def metadata(self, value):
        self.__metadata = value

    @property
    def tarball(self) -> BytesIO:
        """
        Retrieves the tarball containg raw data for the current volume.
        Tries to load it from disk cache if available.
        """
        if self.archive_is_available == False:
            return None

        book_tgz_bytes = None

        run_name = os.getenv("GRIN_DATA_RUN_NAME")
        bucket_name = os.getenv("GRIN_DATA_RAW_BUCKET")
        filename = f"{self.barcode}.tar.gz"

        cache_key = f"{bucket_name}:{run_name}/{filename}"

        # Look for tarball in cache
        with get_cache() as cache:
            try:
                return cache.read(cache_key)
            except KeyError:
                pass

        # Load tarball from cloud storage otherwise
        try:
            response = get_s3_client().get_object(
                Key=f"{run_name}/{filename}",
                Bucket=bucket_name,
            )
            assert response

            book_tgz_bytes = response["Body"].read()
            assert book_tgz_bytes
        except Exception as err:
            raise FileNotFoundError(f"Could not retrieve {filename}") from err

        # Save copy in cache
        with get_cache() as cache:
            cache.set(filename, book_tgz_bytes)

        return BytesIO(book_tgz_bytes)

    @property
    def parsed_tarball(self) -> BookTarballData | None:
        """
        Parses the tarball containing raw data for the current book and returns a BookTarballData object.
        """
        if self.archive_is_available == False:
            return None

        # If available, return copy already loaded in memory
        if self.__parsed_tarball:
            return self.__parsed_tarball

        images = []
        hocr = []
        text = []
        gxml = []
        md5 = []

        #
        # Load tarball from cache or remote storage
        #

        #
        with tarfile.open(fileobj=self.tarball, mode="r:gz") as tar:
            # sorted_members = sorted(tar.getmembers(), key=lambda m: m.name)
            # [!] This assumes members are listed by alphabetical order
            for member in tar.getmembers():

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

        self.__parsed_tarball = BookTarballData(
            images=images,
            hocr=hocr,
            text=text,
            gxml=gxml,
            md5=md5,
        )

        return self.__parsed_tarball

    @property
    def postprocessed_ocr(self) -> dict:
        """
        Getter for `__postprocessed_ocr`.
        Automatically loads post-processed OCR data from disk if available.
        """
        # Return instance already available in-memory, if any
        if self.__postprocessed_ocr:
            return self.__postprocessed_ocr

        input_filepath = Path(OCR_POSTPROCESSING_DIR_PATH, f"{self.barcode}.json")
        data = None

        # Check that file exists and load / parse it
        if not input_filepath.exists():
            raise FileNotFoundError(f"{self.barcode}.json")

        with open(input_filepath, "rb+") as fd:
            data = orjson.loads(fd.read())

        # Check format
        try:
            assert data
            assert isinstance(data, dict)
            assert isinstance(data["stats"], dict)
            assert isinstance(data["text_by_page"], list)
        except Exception as err:
            raise ValueError("Invalid data format.") from err

        # Check page count
        if len(data["text_by_page"]) != len(self.text_by_page):
            raise ValueError("Invalid page count.")

        self.__postprocessed_ocr = data
        return self.__postprocessed_ocr

    @postprocessed_ocr.setter
    def postprocessed_ocr(self, input: dict) -> dict:
        """
        Setter for __postprocessed_ocr.
        Saves post-processed OCR text to disk and makes it available at model level.

        Input will be rejected if:
        - It is not a dict containing two keys: "stats" and "text_by_page".
        - If "stats" is not a dict
        - If keys in "stats" do not match `OCRPostprocessingTrainingDataset.TARGET_TYPES`
        - If "text_by_page" is not a list of strings
        - If the length of "text_by_page" doesn't match the book's page count.

        Saved as:
        - `{OCR_POSTPROCESSING_DIR_PATH}/barcode.json`
        """
        from models.ocr_postprocessing_training_dataset import TARGET_TYPES

        # Check format
        try:
            assert isinstance(input, dict)
            assert isinstance(input["stats"], dict)
            assert isinstance(input["text_by_page"], list)

            for item in input["text_by_page"]:
                assert isinstance(item, str)

            for key, value in input["stats"].items():
                assert isinstance(key, str)
                assert key in TARGET_TYPES
                assert isinstance(value, int)

        except Exception as err:
            raise ValueError("Invalid data format.") from err

        # Check page count
        if len(input["text_by_page"]) != len(self.text_by_page):
            raise ValueError("Invalid page count.")

        # Store to disk
        output_filepath = Path(OCR_POSTPROCESSING_DIR_PATH, f"{self.barcode}.json")

        with open(output_filepath, "wb+") as fd:
            fd.write(orjson.dumps(input))

        self.__postprocessed_ocr = input
        return self.__postprocessed_ocr

    @classmethod
    def get_collection_csv(cls, ignore_cache=False) -> BytesIO:
        """
        Retrieves the collection's "books_latest.csv" file from remote storage or cache.
        """
        collection_csv_bytes = None

        run_name = os.getenv("GRIN_DATA_RUN_NAME")
        bucket_name = os.getenv("GRIN_DATA_META_BUCKET")
        is_gz = os.getenv("GRIN_DATA_META_IS_COMPRESSED", "1") == "1"
        filename = "books_latest.csv" if not is_gz else "books_latest.csv.gz"

        cache_key = f"{bucket_name}:{run_name}/{filename}"

        # Get CSV from cache if available
        if not ignore_cache:
            with get_cache() as cache:
                try:
                    return cache.read(cache_key)  # Returns a file descriptor
                except KeyError:  # Key does not exist
                    pass

        # Load CSV from remote storage otherwise
        try:
            response = get_s3_client().get_object(
                Key=f"{os.getenv("GRIN_DATA_RUN_NAME", "")}/{filename}",
                Bucket=os.getenv("GRIN_DATA_META_BUCKET"),
            )
            assert response

            collection_csv_bytes = response["Body"].read()

            if is_gz:
                collection_csv_bytes = gzip.decompress(collection_csv_bytes)

            assert collection_csv_bytes
        except Exception as err:
            raise FileNotFoundError(f"Could not retrieve {filename}") from err

        # Save copy in cache
        with get_cache() as cache:
            cache.add(cache_key, collection_csv_bytes)

        return BytesIO(collection_csv_bytes)
