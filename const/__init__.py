import os
from pathlib import Path
from datetime import datetime, timezone

from slugify import slugify
from dotenv import load_dotenv

load_dotenv()

#
# Data directory
#
DATA_DIR_PATH = os.environ.get("DATA_DIR_PATH", "data/")
""" Data directory: root path. """

CACHE_DIR_PATH = Path(DATA_DIR_PATH, "cache")

DATABASE_DIR_PATH = Path(DATA_DIR_PATH, "database")

DATABASE_FILENAME = "database.db"

EXPORT_DIR_PATH = Path(DATA_DIR_PATH, "export")

MODELS_DIR_PATH = Path(DATA_DIR_PATH, "models")

MISC_DIR_PATH = Path(DATA_DIR_PATH, "misc")

HF_DATASET_DIR_PATH = Path(DATA_DIR_PATH, "hf-dataset")

OCR_POSTPROCESSING_DIR_PATH = Path(DATA_DIR_PATH, "ocr-postprocessing")

PD_EXCLUSION_LIST_FILEPATH = Path(DATA_DIR_PATH, "pd-exclusion-list.txt")

#
# Pipeline ready check
#
READY_CHECK_FILEPATH = Path(DATA_DIR_PATH, "ready.check")
""" File used to indicate that the data is ready to be analyzed. Written by `data build`. """

#
# Misc
#
DATETIME_SLUG = datetime_slug = slugify(
    datetime.now(timezone.utc).isoformat(sep=" ", timespec="minutes")
)
""" Datetime slug. Hoisted at `const` level for convenience. """

DEFAULT_SIMHASH_SHINGLE_WIDTH = 7
""" Default size of Simhash shingles. """

HATHITRUST_PD_CODES = ["pd", "pdus", "cc-zero"]
""" Values of Hathitrust's "rights_code" field that match with public domain. """

HATHITRUST_PD_STRING = "Full view"
""" Value of Hathitrust's "us_rights_string" field that match with full online access. """
