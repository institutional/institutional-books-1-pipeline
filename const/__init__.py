import os
from pathlib import Path
import datetime

from slugify import slugify
from dotenv import load_dotenv

load_dotenv()

#
# Data directory
#
DATA_DIR_PATH = os.environ.get("DATA_DIR_PATH", None)
""" Data directory: root path. """

INPUT_DIR_PATH = Path(DATA_DIR_PATH, "input")
""" Data input directory. """

OUTPUT_DIR_PATH = Path(DATA_DIR_PATH, "output")
""" Data output directory. """

INPUT_JSONL_DIR_PATH = Path(INPUT_DIR_PATH, "jsonl")
""" Data input directory: JSONL folder. """

INPUT_CSV_DIR_PATH = Path(INPUT_DIR_PATH, "csv")
""" Data input directory: CSV folder. """

INPUT_MISC_DIR_PATH = Path(INPUT_DIR_PATH, "misc")
""" Data input directory: misc folder. Can be used for temporary files. """

OUTPUT_DATABASE_DIR_PATH = Path(OUTPUT_DIR_PATH, "database")
""" Data output directory: database folder. """

OUTPUT_DATABASE_FILENAME = "database.db"
""" Filename for the local database. """

OUTPUT_EXPORT_DIR_PATH = Path(OUTPUT_DIR_PATH, "export")
""" Data output directory: export folder. """

OUTPUT_MISC_DIR_PATH = Path(OUTPUT_DIR_PATH, "misc")
""" Data output directory: misc folder. Can be used for temporary files. """

#
# Pipeline ready check
#
OUTPUT_PIPELINE_READY_FILEPATH = Path(DATA_DIR_PATH, "ready.check")
""" File used to indicate that the data is ready to be analyzed. Written by `data build`. """

#
# Target tranche settings
#
GRIN_TO_S3_TRANCHES = [
    "VIEW_FULL",
    "VIEW_SNIPPET",
    "VIEW_NONE",
    "VIEW_METADATA",
    "MISSING",
]
""" GRIN TO S3: Available tranches. """

GRIN_TO_S3_TRANCHES_TO_BUCKET_NAMES = {
    "VIEW_FULL": "gbooks-primary",
    "VIEW_SNIPPET": "gbooks-clearance",
    "VIEW_NONE": "gbooks-clearance",
    "VIEW_METADATA": "gbooks-clearance",
    "MISSING": "gbooks-clearance",
}
""" GRIN TO S3: Available tranches and buckets hosting them. """

GRIN_TO_S3_BUCKET_NAMES_TO_TRANCHES = {
    "gbooks-primary": ["VIEW_FULL"],
    "gbooks-clearance": ["VIEW_SNIPPET", "VIEW_NONE", "VIEW_METADATA", "MISSING"],
}
""" GRIN TO S3: Available buckets and the tranches they host. """

GRIN_TO_S3_BUCKET_VERSION_PREFIX = "v2"
""" GRIN TO S3: Version of the export to be used. Determines path for files (i.e: v1/jsonl/foo.jsonl) """

#
# Misc
#
DATETIME_SLUG = datetime_slug = slugify(
    datetime.datetime.utcnow().isoformat(sep=" ", timespec="minutes"),
)
""" Datetime slug. Hoisted at `const` level for convenience. """

TABLES = {
    "book_io": "BookIO",
    "token_count": "TokenCount",
    "page_count": "PageCount",
    "year_of_publication": "YearOfPublication",
    "main_language": "MainLanguage",
    "topic_classification": "TopicClassification",
    "genre_classification": "GenreClassification",
    "ocr_quality": "OCRQuality",
    "scanned_text_simhash": "ScannedTextSimhash",
    "language_detection": "LanguageDetection",
}
""" Mapping: Table names to Model class name. """

DEFAULT_SIMHASH_SHINGLE_WIDTH = 7
""" Default size of Simhash shingles. """
