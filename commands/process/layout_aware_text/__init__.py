import click

from .step01_extract_layout_data import step01_extract_layout_data


@click.group("layout-aware-text")
def layout_aware_text():
    """Command group: Generating layout-aware text from OCR metadata."""
    pass


layout_aware_text.add_command(step01_extract_layout_data)
