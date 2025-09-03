import os


def check_env() -> None:
    """
    Throws if any of the required env vars is not set.
    """
    for env_var in [
        "DATA_DIR_PATH",
        "CACHE_MAX_SIZE_IN_GB",
        "GRIN_DATA_ENDPOINT",
        "GRIN_DATA_ACCESS_KEY_ID",
        "GRIN_DATA_SECRET_ACCESS_KEY",
        "GRIN_DATA_RAW_BUCKET",
        "GRIN_DATA_FULL_BUCKET",
        "GRIN_DATA_META_BUCKET",
        "GRIN_DATA_RUN_NAME",
        "GRIN_DATA_FULL_IS_COMPRESSED",
        "GRIN_DATA_META_IS_COMPRESSED",
        "PD_FILTERING_MECHANISM",
    ]:
        try:
            assert os.environ.get(env_var, None)
            assert os.environ.get(env_var, "").strip()
        except Exception:
            raise KeyError(f"Required environment variable {env_var} not found.")
