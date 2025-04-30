import os

from const import (
    DATA_DIR_PATH,
    INPUT_DIR_PATH,
    OUTPUT_DIR_PATH,
    INPUT_JSONL_DIR_PATH,
    INPUT_CSV_DIR_PATH,
    INPUT_MISC_DIR_PATH,
    INPUT_CACHE_DIR_PATH,
    OUTPUT_DATABASE_DIR_PATH,
    OUTPUT_EXPORT_DIR_PATH,
    OUTPUT_MODELS_DIR_PATH,
    OUTPUT_MISC_DIR_PATH,
    OUTPUT_OCR_POSTPROCESSING_DIR_PATH,
)


def make_dirs() -> None:
    """
    Creates target dirs as needed.
    Throws if any of the target destinations cannot be written.
    """
    os.makedirs(DATA_DIR_PATH, exist_ok=True)
    os.makedirs(INPUT_DIR_PATH, exist_ok=True)
    os.makedirs(OUTPUT_DIR_PATH, exist_ok=True)

    os.makedirs(INPUT_JSONL_DIR_PATH, exist_ok=True)
    os.makedirs(INPUT_CSV_DIR_PATH, exist_ok=True)
    os.makedirs(INPUT_CSV_DIR_PATH, exist_ok=True)
    os.makedirs(INPUT_MISC_DIR_PATH, exist_ok=True)
    os.makedirs(INPUT_CACHE_DIR_PATH, exist_ok=True)

    os.makedirs(OUTPUT_DATABASE_DIR_PATH, exist_ok=True)
    os.makedirs(OUTPUT_EXPORT_DIR_PATH, exist_ok=True)
    os.makedirs(OUTPUT_MODELS_DIR_PATH, exist_ok=True)
    os.makedirs(OUTPUT_MISC_DIR_PATH, exist_ok=True)
    os.makedirs(OUTPUT_OCR_POSTPROCESSING_DIR_PATH, exist_ok=True)
