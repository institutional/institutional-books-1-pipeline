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
    "RUNNING_HEAD_FULL",
    "RUNNING_HEAD_CHUNK",
    "HEADING_OR_TITLE_FULL",
    "HEADING_OR_TITLE_CHUNK",
    "PARAGRAPH_FULL",
    "PARAGRAPH_CHUNK",
    "PARAGRAPH_START",
    "PARAGRAPH_END",
    "FOOTNOTE_FULL",
    "FOOTNOTE_CHUNK",
    "FOOTNOTE_START",
    "FOOTNOTE_END",
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

    page = peewee.IntegerField(
        null=True,
        unique=False,
        index=True,
    )

    order = peewee.IntegerField(
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

    target_type_average_linear_logprob = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
    )

    target_type_perplexity = peewee.FloatField(
        null=True,
        unique=False,
        index=False,
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
        Returns the current chunk in a format suitable for training / inference for classification with a static model.
        Example:
        ```
        <<12,5>> Hello world
        ```
        """
        return f"<<{self.page},{self.order}>> {self.text}"

    @classmethod
    def get_auto_annotation_repr(
        cls,
        current,
        previous=None,
        next=None,
        total_chunks_for_page: int = 0,
    ) -> str:
        """
        Returns a textual representation for an OCR chunk, in context (page info, previous and next chunk).
        This representation can be used to generate training data with an text-generation model.

        Example:
        ```
        <context>Page 12 of 320, Chunk 4 of 128</context>
        <previous>Lorem ipsum </previous>
        <current>dolor sit</current>
        <next>amet.</next>
        ```
        """
        output = ""

        # Context
        output += f"<context>"
        output += f"Page {current.page+1} of {current.book.pagecount_set[0].count_from_ocr}, "

        if total_chunks_for_page:
            output += f"Chunk {current.order+1} of {total_chunks_for_page}, "
        else:
            output += f"Chunk {current.order+1}, "

        output += f"Language: {current.book.mainlanguage_set[0].from_detection_iso639_3}"
        output += f"</context>\n"

        # Previous chunk
        if previous:
            output += f"<previous>{previous.text}</previous>\n"

        # Current chunk
        output += f"<current>{current.text}</current>\n"

        # Next chunk
        if next:
            output += f"<next>{next.text}</next>"

        return output

    @classmethod
    def get_chunks_from_page(cls, book: BookIO, page: int) -> list:
        """
        Splits the text of a page into a list of (unsaved and untyped) OCR chunks as OCRPostprocessingTrainingDataset objects.
        """
        assert isinstance(book, BookIO)
        text = book.text[page]

        output = []

        for i, text_chunk in enumerate(text.split("\n")):
            item = OCRPostprocessingTrainingDataset()
            item.book = book.barcode
            item.page = page
            item.order = i
            item.text = text_chunk

            output.append(item)

        return output
