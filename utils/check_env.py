import os


def check_env() -> None:
    """
    Throws if any of the required env vars is not set.
    """
    for env_var in [
        "DATA_DIR_PATH",
        "CACHE_MAX_SIZE_IN_GB",
        "GRIN_TO_S3_DATA_ENDPOINT",
        "GRIN_TO_S3_DATA_ACCESS_KEY_ID",
        "GRIN_TO_S3_DATA_SECRET_ACCESS_KEY",
    ]:
        try:
            assert os.environ.get(env_var, None)
        except Exception:
            raise KeyError(f"Required environment variable {env_var} not found.")
