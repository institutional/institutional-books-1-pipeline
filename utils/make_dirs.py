import os

from const import (
    DATA_DIR_PATH,
    CACHE_DIR_PATH,
    DATABASE_DIR_PATH,
    EXPORT_DIR_PATH,
    MODELS_DIR_PATH,
    MISC_DIR_PATH,
    OCR_POSTPROCESSING_DIR_PATH,
    HF_DATASET_DIR_PATH,
)


def make_dirs() -> None:
    """
    Creates target dirs as needed.
    Throws if any of the target destinations cannot be written.
    """
    os.makedirs(DATA_DIR_PATH, exist_ok=True)

    os.makedirs(CACHE_DIR_PATH, exist_ok=True)

    os.makedirs(DATABASE_DIR_PATH, exist_ok=True)
    os.makedirs(EXPORT_DIR_PATH, exist_ok=True)
    os.makedirs(MODELS_DIR_PATH, exist_ok=True)
    os.makedirs(MISC_DIR_PATH, exist_ok=True)
    os.makedirs(OCR_POSTPROCESSING_DIR_PATH, exist_ok=True)
    os.makedirs(HF_DATASET_DIR_PATH, exist_ok=True)
