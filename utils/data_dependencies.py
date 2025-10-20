import os

from loguru import logger

from const import OCR_POSTPROCESSING_DIR_PATH


def needs_page_count_data(func):
    """
    Decorator conditioning the execution of a function to:
    - The presence of page count data in the database.
    """
    from models import BookIO, PageCount

    def wrapper(*args, **kwargs):

        try:
            assert BookIO.select().count() == PageCount.select().count()
        except:
            logger.error("Page count data is not available.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def needs_text_analysis_data(func):
    """
    Decorator conditioning the execution of a function to:
    - The presence of text analysis data in the database.
    """
    from models import BookIO, TextAnalysis

    def wrapper(*args, **kwargs):

        try:
            assert BookIO.select().count() == TextAnalysis.select().count()
        except:
            logger.error("Text analysis data is not available.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def needs_main_language_data(func):
    """
    Decorator conditioning the execution of a function to:
    - The presence of main language data in the database.
    """
    from models import BookIO, MainLanguage

    def wrapper(*args, **kwargs):

        try:
            assert BookIO.select().count() == MainLanguage.select().count()
        except:
            logger.error("This command needs main language data.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def needs_language_detection_data(func):
    """
    Decorator conditioning the execution of a function to:
    - The presence of language detection data in the database.
    """
    from models import BookIO, MainLanguage, LanguageDetection

    def wrapper(*args, **kwargs):

        try:
            assert BookIO.select().count() == MainLanguage.select().count()

            count = (
                MainLanguage.select()
                .where(MainLanguage.from_detection_iso639_3.is_null(False))
                .count()
            )
            assert count

            count = None
            count = LanguageDetection.select().count()
            assert count
        except:
            logger.error("This command needs language detection data.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def needs_ocr_quality_data(func):
    """
    Decorator conditioning the execution of a function to:
    - The presence of OCR quality data in the database.
    """
    from models import BookIO, OCRQuality

    def wrapper(*args, **kwargs):

        try:
            assert BookIO.select().count() == OCRQuality.select().count()
        except:
            logger.error("This command needs OCR quality data.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def needs_scanned_text_simhash_data(func):
    """
    Decorator conditioning the execution of a function to:
    - The presence of scanned text simhash data in the database.
    """
    from models import BookIO, ScannedTextSimhash

    def wrapper(*args, **kwargs):

        try:
            assert BookIO.select().count() == ScannedTextSimhash.select().count()
        except:
            logger.error("This command needs scanned text simhash data.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def needs_hathitrust_collection_prefix(func):
    """
    Decorator conditioning the execution of a function to:
    The presence and validity of the HATHITRUST_COLLECTION_PREFIX env var.
    """

    def wrapper(*args, **kwargs):

        try:
            ht_collection_prefix = os.getenv("HATHITRUST_COLLECTION_PREFIX", None)
            assert ht_collection_prefix is not None
            assert len(str(ht_collection_prefix)) == 3
        except:
            logger.error("HATHITRUST_COLLECTION_PREFIX env var must be set.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def needs_everything(func):
    """
    Decorator conditioning the execution of a function to:
    The completion of all analysis and processing steps.

    This is helpful before running export or publish commands.
    """
    from models import (
        BookIO,
        GenreClassification,
        HathitrustRightsDetermination,
        MainLanguage,
        LanguageDetection,
        OCRPostprocessingTrainingDataset,
        OCRQuality,
        PageCount,
        ScannedTextSimhash,
        TextAnalysis,
        TokenCount,
        TopicClassificationTrainingDataset,
        TopicClassification,
        YearOfPublication,
    )

    def wrapper(*args, **kwargs):

        book_count = BookIO.select().count()

        if not book_count:
            logger.error("No books available.")
            exit(1)

        # Database records check
        try:
            assert book_count == GenreClassification.select().count()
        except:
            logger.error("Genre classification data is missing.")
            exit(1)

        try:
            assert book_count == HathitrustRightsDetermination.select().count()
        except:
            logger.error("Hathitrust rights determination data is missing.")
            exit(1)

        try:
            assert book_count == MainLanguage.select().count()
            assert (
                MainLanguage.select()
                .where(MainLanguage.from_detection_iso639_3.is_null(False))
                .count()
            )
        except:
            logger.error("Main language data is missing.")
            exit(1)

        try:
            assert LanguageDetection.select().count() > book_count
        except:
            logger.error("Language detection data is missing.")
            exit(1)

        try:
            assert OCRPostprocessingTrainingDataset.select().count()
        except:
            logger.error("OCR Post processing dataset data is missing.")
            exit(1)

        try:
            assert book_count == OCRQuality.select().count()
            assert OCRQuality.select().where(OCRQuality.from_metadata.is_null(False)).count()
            assert OCRQuality.select().where(OCRQuality.from_detection.is_null(False)).count()
        except:
            logger.error("OCR quality data is missing.")
            exit(1)

        try:
            assert book_count == PageCount.select().count()
            assert PageCount.select().where(PageCount.count_from_ocr.is_null(False)).count()
        except:
            logger.error("Page count data is missing.")
            exit(1)

        try:
            assert book_count == ScannedTextSimhash.select().count()
        except:
            logger.error("Scanned text simhash data is missing.")
            exit(1)

        try:
            assert book_count == TextAnalysis.select().count()
        except:
            logger.error("Text analysis data is missing.")
            exit(1)

        try:
            assert TokenCount.select().count() > book_count
        except:
            logger.error("Token count data is missing.")
            exit(1)

        try:
            assert TopicClassificationTrainingDataset.select().count()
        except:
            logger.error("Topic classification training dataset data is missing.")
            exit(1)

        try:
            assert book_count == TopicClassification.select().count()
            assert (
                TopicClassification.select()
                .where(TopicClassification.from_detection.is_null(False))
                .count()
            )
            assert (
                TopicClassification.select()
                .where(TopicClassification.from_metadata.is_null(False))
                .count()
            )
        except:
            logger.error("Topic classification data is missing.")
            exit(1)

        try:
            assert book_count == YearOfPublication.select().count()
        except:
            logger.error("Year of publication data is missing.")
            exit(1)

        # Check presence of local files
        """
        try:
            assert len(glob.glob(f"{OCR_POSTPROCESSING_DIR_PATH}/*.json")) > 0
        except:
            logger.error("OCR postprocessing data is missing.")
            exit(1)
        """

        return func(*args, **kwargs)

    return wrapper
