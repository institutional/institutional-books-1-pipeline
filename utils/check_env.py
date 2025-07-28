import os


def check_env() -> None:
    """
    Throws if any of the required env vars is not set.
    """
    # Always required variables
    always_required = [
        "DATA_DIR_PATH",  
        "CACHE_MAX_SIZE_IN_GB",
    ]
    
    # S3-only variables (only required when using S3 source)
    s3_required = [
        "GRIN_TO_S3_DATA_ENDPOINT",
        "GRIN_TO_S3_DATA_ACCESS_KEY_ID", 
        "GRIN_TO_S3_DATA_SECRET_ACCESS_KEY",
    ]
    
    # Check always required variables
    for env_var in always_required:
        try:
            assert os.environ.get(env_var, None)
        except Exception:
            raise KeyError(f"Required environment variable {env_var} not found.")
    
    # Check S3 variables only if they seem to be configured
    # (This allows HuggingFace-only usage without S3 credentials)
    s3_configured = any(os.environ.get(var, "").strip() for var in s3_required)
    if s3_configured:
        # If any S3 var is set, require all of them
        for env_var in s3_required:
            try:
                assert os.environ.get(env_var, None)
            except Exception:
                raise KeyError(f"Required S3 environment variable {env_var} not found. Either set all S3 variables or leave them empty to use HuggingFace source.")
