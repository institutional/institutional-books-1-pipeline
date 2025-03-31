import os

import diskcache

from const import CACHE_DIR_PATH


def get_cache() -> diskcache.Cache:
    """
    Returns a ready-to-use on-disk cache instance.
    """
    directory = str(CACHE_DIR_PATH)
    size_limit = int(os.getenv("CACHE_MAX_SIZE_IN_GB", 1)) * 1_000_000_000

    return diskcache.Cache(directory=directory, size_limit=size_limit)
