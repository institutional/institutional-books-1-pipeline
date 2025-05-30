import glob

import click

from const import OUTPUT_OCR_POSTPROCESSING_DIR_PATH


def needs_hathitrust_rights_determination_data(func):
    """
    Decorator conditioning the execution of a function to:
    - The presence of Hathitrust rights determination data in the database.
    """
    from models import BookIO, HathitrustRightsDetermination

    def wrapper(*args, **kwargs):

        try:
            assert BookIO.select().count() == HathitrustRightsDetermination.select().count()
        except:
            click.echo("Hathitrust rights determination data is not available.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


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
            click.echo("Page count data is not available.")
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
            click.echo("Text analysis data is not available.")
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
            click.echo("This command needs main language data.")
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
            click.echo("This command needs language detection data.")
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
            click.echo("This command needs OCR quality data.")
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
            click.echo("This command needs scanned text simhash data.")
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
            click.echo("No books available.")
            exit(1)

        # Database records check
        try:
            assert book_count == GenreClassification.select().count()
        except:
            click.echo("Genre classification data is missing.")
            exit(1)

        try:
            assert book_count == HathitrustRightsDetermination.select().count()
        except:
            click.echo("Hathitrust rights determination data is missing.")
            exit(1)

        try:
            assert book_count == MainLanguage.select().count()
            assert (
                MainLanguage.select()
                .where(MainLanguage.from_detection_iso639_3.is_null(False))
                .count()
            )
        except:
            click.echo("Main language data is missing.")
            exit(1)

        try:
            assert LanguageDetection.select().count() > book_count
        except:
            click.echo("Language detection data is missing.")
            exit(1)

        try:
            assert OCRPostprocessingTrainingDataset.select().count()
        except:
            click.echo("OCR Post processing dataset data is missing.")
            exit(1)

        try:
            assert book_count == OCRQuality.select().count()
            assert OCRQuality.select().where(OCRQuality.from_metadata.is_null(False)).count()
            assert OCRQuality.select().where(OCRQuality.from_detection.is_null(False)).count()
        except:
            click.echo("OCR quality data is missing.")
            exit(1)

        try:
            assert book_count == PageCount.select().count()
            assert PageCount.select().where(PageCount.count_from_metadata.is_null(False)).count()
            assert PageCount.select().where(PageCount.count_from_ocr.is_null(False)).count()
        except:
            click.echo("Page count data is missing.")
            exit(1)

        try:
            assert book_count == ScannedTextSimhash.select().count()
        except:
            click.echo("Scanned text simhash data is missing.")
            exit(1)

        try:
            assert book_count == TextAnalysis.select().count()
        except:
            click.echo("Text analysis data is missing.")
            exit(1)

        try:
            assert TokenCount.select().count() > book_count
        except:
            click.echo("Token count data is missing.")
            exit(1)

        try:
            assert TopicClassificationTrainingDataset.select().count()
        except:
            click.echo("Topic classification training dataset data is missing.")
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
            click.echo("Topic classification data is missing.")
            exit(1)

        try:
            assert book_count == YearOfPublication.select().count()
        except:
            click.echo("Year of publication data is missing.")
            exit(1)

        # Check presence of local files
        """
        try:
            assert len(glob.glob(f"{OUTPUT_OCR_POSTPROCESSING_DIR_PATH}/*.json")) > 0
        except:
            click.echo("OCR postprocessing data is missing.")
            exit(1)
        """

        return func(*args, **kwargs)

    return wrapper
