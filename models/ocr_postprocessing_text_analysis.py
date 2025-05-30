import peewee

from utils import get_db

from models import TextAnalysis


class OCRPostProcessingTextAnalysis(TextAnalysis):
    """
    `ocr_postprocessing_text_analysis` table:
    - Keeps track of text analysis metrics on post-processed OCR'd texts.
    - Specialized mirror of `text_analysis`.
    """

    class Meta:
        table_name = "ocr_postprocessing_text_analysis"
        database = get_db()
