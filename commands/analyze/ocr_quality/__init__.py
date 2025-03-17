import click

from .step01_extract_from_metadata import step01_extract_from_metadata
from .step02_detect import step02_detect


@click.group("ocr-quality")
def ocr_quality():
    """
    Command group: OCR quality analysis.
    """
    pass


ocr_quality.add_command(step01_extract_from_metadata)
ocr_quality.add_command(step02_detect)
