import boto3
from botocore.config import Config

import os


def get_s3_client():
    """
    Returns an S3 client connected to the upstream account hosting the raw corpus.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("GRIN_DATA_ENDPOINT"),
        aws_access_key_id=os.environ.get("GRIN_DATA_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("GRIN_DATA_SECRET_ACCESS_KEY"),
        config=Config(
            region_name=os.environ.get("GRIN_DATA_REGION", "auto"),
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
