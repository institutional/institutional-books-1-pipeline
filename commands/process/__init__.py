from .ocr_postprocessing import ocr_postprocessing

import click


@click.group("process")
def process():
    """Command group: Corpus processing / augmentation."""
    pass


process.add_command(ocr_postprocessing)
