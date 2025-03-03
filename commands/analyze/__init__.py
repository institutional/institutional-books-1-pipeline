import click

from .run_token_count import run_token_count
from .run_page_count import run_page_count
from .extract_year_of_publication_from_metadata import extract_year_of_publication_from_metadata
from .topic_classification import topic_classification
from .genre_classification import genre_classification
from .language import language
from .ocr_quality import ocr_quality


@click.group("analyze")
def analyze():
    """Command group: Corpus analysis."""
    pass


analyze.add_command(run_token_count)
analyze.add_command(run_page_count)
analyze.add_command(extract_year_of_publication_from_metadata)
analyze.add_command(topic_classification)
analyze.add_command(genre_classification)
analyze.add_command(language)
analyze.add_command(ocr_quality)
