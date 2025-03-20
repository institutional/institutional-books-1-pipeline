import click

from .extract_genre_from_metadata import extract_genre_from_metadata
from .extract_main_language_from_metadata import extract_main_language_from_metadata
from .extract_ocr_quality_from_metadata import extract_ocr_quality_from_metadata
from .extract_page_count import extract_page_count
from .extract_topic_from_metadata import extract_topic_from_metadata
from .extract_year_of_publication_from_metadata import extract_year_of_publication_from_metadata

from .run_language_detection import run_language_detection
from .run_ocr_quality_detection import run_ocr_quality_detection
from .run_simhash import run_simhash
from .run_token_count import run_token_count


@click.group("analyze")
def analyze():
    """Command group: Corpus analysis."""
    pass


analyze.add_command(extract_genre_from_metadata)
analyze.add_command(extract_main_language_from_metadata)
analyze.add_command(extract_ocr_quality_from_metadata)
analyze.add_command(extract_page_count)
analyze.add_command(extract_topic_from_metadata)
analyze.add_command(extract_year_of_publication_from_metadata)

analyze.add_command(run_language_detection)
analyze.add_command(run_ocr_quality_detection)
analyze.add_command(run_simhash)
analyze.add_command(run_token_count)
