import os
from pathlib import Path

from loguru import logger

from const import HATHITRUST_PD_CODES, HATHITRUST_PD_STRING, PD_EXCLUSION_LIST_FILEPATH

EXCLUSION_LIST = set()
""" Module-level memory cache for exclusion list contents. """


def is_pd(book) -> bool:
    """
    Uses the rights determination mechanism set by `PD_FILTERING_MECHANISM` to determine if a given volume is likely to be in the public domain/permissively licensed or not.
    """
    from models import BookIO

    result = None

    #
    # Hathitrust
    #
    if os.getenv("PD_FILTERING_MECHANISM", None) == "HATHITRUST":
        try:
            rights_determination = book.hathitrustrightsdetermination_set[0]
            assert rights_determination.rights_code in HATHITRUST_PD_CODES
            assert rights_determination.us_rights_string == HATHITRUST_PD_STRING
            result = True
        except:
            result = False

    #
    # Exclusion list
    #
    if os.getenv("PD_FILTERING_MECHANISM", None) == "LIST":
        # Populate module level memory cache if need be
        if not EXCLUSION_LIST:
            with open(PD_EXCLUSION_LIST_FILEPATH, "r+") as fd:
                [EXCLUSION_LIST.add(item) for item in fd.read().split("\n")]

        if not EXCLUSION_LIST:
            logger.warning("PD exclusion list is empty")

        result = book.barcode not in EXCLUSION_LIST

    if result is None:
        raise Exception(
            "`PD_FILTERING_MECHANISM` is invalid, or a determination could not be made."
        )

    return result
