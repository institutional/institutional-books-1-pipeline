import peewee

from utils import get_db

from models import BookIO

SET_TYPES = [
    ("train", "train"),
    ("test", "test"),
]

TARGET_TYPES = [
    "UNKNOWN",
    "NOISE_OR_BROKEN_TEXT",
    "PAGE_NUMBER",
    "RUNNING_HEAD",
    "HEADING_OR_TITLE",
    "PARAGRAPH_CHUNK",
    "PARAGRAPH_END",
    "LOOSE_SENTENCE_OR_LIST_ITEM",
    "SEPARATOR",
]
""" List of possible OCR chunk types. """


class OCRPostprocessingTrainingDataset(peewee.Model):
    """
    `ocr_postprocessing_training_dataset` table:
    Labelled OCR "lines" to be used to training a postprocessing model.
    """

    class Meta:
        table_name = "ocr_postprocessing_training_dataset"
        database = get_db()

    ocr_postprocessing_training_dataset_id = peewee.PrimaryKeyField()

    book = peewee.ForeignKeyField(
        model=BookIO,
        field="barcode",
        index=True,
    )

    page_number = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    total_pages = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    line_number = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    total_lines = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    text = peewee.TextField(
        null=True,
        unique=False,
        index=False,
    )

    target_type = peewee.CharField(
        null=True,
        unique=False,
        index=True,
    )

    set = peewee.CharField(
        max_length=256,
        null=True,
        unique=False,
        index=True,
        choices=SET_TYPES,
    )

    def get_training_repr(self) -> str:
        """
        Returns a representation of the current OCR chunk for classification training and inference purposes.

        Example:
        ```
        <<12-45,5-456>> Hello world
        ```
        """
        prefix = f"<<{self.page_number}-{self.total_pages},{self.line_number}-{self.total_lines}>>"

        return f"{prefix} {self.text}"

    @classmethod
    def get_chunks_from_page(cls, book: BookIO, page: int) -> list:
        """
        Splits the text of a page into a list of (unsaved and untyped) OCR chunks as OCRPostprocessingTrainingDataset objects.
        Simple line-by-line split (matches the collection's formatting).
        """
        assert isinstance(book, BookIO)
        text = book.text[page]

        output = []
        lines = text.split("\n")

        for i, text_chunk in enumerate(text.split("\n")):
            item = OCRPostprocessingTrainingDataset()
            item.book = book.barcode
            item.page_number = page

            if book.pagecount_set:
                item.total_pages = book.pagecount_set[0].count_from_ocr
            else:
                item.total_pages = book.csv_data["Page Count"]

            item.line_number = i
            item.total_lines = len(lines)

            item.text = text_chunk

            output.append(item)

        return output
