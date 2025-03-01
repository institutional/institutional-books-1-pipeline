import os


def check_env() -> None:
    """
    Throws if any of the required env vars is not set.
    """
    for env_var in [
        "DATA_DIR_PATH",
        "GRIN_TO_S3_DATA_ENDPOINT",
        "GRIN_TO_S3_DATA_ACCESS_KEY_ID",
        "GRIN_TO_S3_DATA_SECRET_ACCESS_KEY",
        "OPENAI_API_KEY",
        "OPENAI_ORG_ID",
        "HF_TOKEN",
    ]:
        try:
            assert os.environ.get(env_var, None)
        except Exception:
            raise KeyError(f"Required environment variable {env_var} not found.")
