import io
import json
import logging
import pandas as pd

import boto3
from botocore.client import Config

from src.config import settings

logger = logging.getLogger(__name__)


def create_minio_client():
    """
    Create S3 client connected to local MinIO.
    """

    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config( signature_version="s3v4" ),
        region_name="us-east-1",
    )


def upload_json(
    data: dict,
    object_key: str,
    bucket: str,
) -> None:
    """
    Upload JSON object to MinIO.
    """

    client = create_minio_client()

    json_data = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
    )

    client.upload_fileobj(
        io.BytesIO(
            json_data.encode("utf-8")
        ),
        bucket,
        object_key,
        ExtraArgs={ "ContentType": "application/json", },
    )

    logger.info(
        "Uploaded object: s3://%s/%s",
        bucket,
        object_key,
    )

def download_json(
    object_key: str,
    bucket: str,
) -> dict:
    """Download JSON object from MinIO."""

    client = create_minio_client()

    response = client.get_object(
        Bucket=bucket,
        Key=object_key,
    )

    try:
        data = response["Body"].read()
    finally:
        response["Body"].close()

    return json.loads(
        data.decode("utf-8")
    )

def upload_parquet(
    dataframe: pd.DataFrame,
    object_key: str,
    bucket: str,
) -> None:
    """Upload pandas DataFrame as Parquet."""

    client = create_minio_client()

    parquet_buffer = io.BytesIO()

    dataframe.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
    )

    parquet_buffer.seek(0)

    client.upload_fileobj(
        parquet_buffer,
        bucket,
        object_key,
        ExtraArgs={
            "ContentType": "application/octet-stream",
        },
    )

    logger.info(
        "Uploaded Parquet: s3://%s/%s",
        bucket,
        object_key,
    )

def download_parquet(
    object_key: str,
    bucket: str,
) -> pd.DataFrame:
    """Download Parquet object from MinIO."""

    client = create_minio_client()

    response = client.get_object(
        Bucket=bucket,
        Key=object_key,
    )

    try:
        data = response["Body"].read()
    finally:
        response["Body"].close()

    return pd.read_parquet(
        io.BytesIO(data),
        engine="pyarrow",
    )

def list_objects(
    prefix: str,
    bucket: str,
) -> list[str]:
    """List objects in MinIO under a prefix."""

    client = create_minio_client()

    paginator = client.get_paginator(
        "list_objects_v2"
    )

    object_keys = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            object_keys.append(
                obj["Key"]
            )

    logger.info(
        "Found %d objects under '%s'",
        len(object_keys),
        prefix,
    )

    return object_keys
