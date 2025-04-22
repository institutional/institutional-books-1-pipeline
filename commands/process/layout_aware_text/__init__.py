import click

from .step01_extract_layout_data import step01_extract_layout_data
from .step02_identify_complex_layouts import step02_identify_complex_layouts
from .step03_assemble_spans import step03_assemble_spans
from .step04_sort_spans import step04_sort_spans


@click.group("layout-aware-text")
def layout_aware_text():
    """Command group: Generating layout-aware text from OCR metadata."""
    pass


layout_aware_text.add_command(step01_extract_layout_data)
layout_aware_text.add_command(step02_identify_complex_layouts)
layout_aware_text.add_command(step03_assemble_spans)
layout_aware_text.add_command(step04_sort_spans)
