import os
import json
import csv
from pathlib import Path

import peewee

from utils import get_db
from const import INPUT_JSONL_DIR_PATH, INPUT_CSV_DIR_PATH


class BookIO(peewee.Model):
    """
    `book_io` table: Organizes information about each JSONL entry (individual books).
    Stores offset information to allow for random access.
    """

    def __init__(self, *args, **kwargs):
        super(BookIO, self).__init__(*args, **kwargs)
        self.__jsonl_data = None
        self.__csv_data = None

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

                csv_data = csv.DictReader(
                    [csv_line],
                    BookIO.__book_csv_headers,
                ).__next__()

                self.__csv_data = csv_data

        return self.__csv_data

    @csv_data.setter
    def csv_data(self, value):
        self.__csv_data = value

    @property
    def merged_text(self) -> str:
        """
        Returns the full OCR'd text of the current book merged as a single string
        """
        return "\n".join(self.jsonl_data["text_by_page"])

    @property
    def continuous_character_count(self) -> int:
        """
        Returns the total number of "continous" characters in the OCR'd text of the current book.
        This attempts to exclude line breaks and spaces.
        """
        return len(
            self.merged_text.replace(" ", "")
            .replace("\n", "")
            .replace("\t", "")
            .replace("\u200b", "")
            .replace("-", "")
            .replace("—", "")
        )
