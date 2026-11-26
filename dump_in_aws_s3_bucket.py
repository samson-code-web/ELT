from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import boto3
import io
import logging

logger = logging.getLogger(f"elt.{__name__}")
config = TransferConfig(
    multipart_chunksize= 1 * 1024 * 1024,
    multipart_threshold= 16 * 1024 * 1024,
    max_concurrency=5,
    use_threads=True
    )

def send_to_bucket(file: io.BytesIO | None):

    if file is not None:
        try:
            s3_client = boto3.Session(profile_name="samson-elt")
            client = s3_client.client("s3")
            logger.info(f"Sending file to bucket")
            client.upload_fileobj(Fileobj=file, Bucket="elt-github-623955527676-ca-central-1-an", Key="elt/raw/github.parquet", Config=config)
            logger.info('done uploading file to bucket')

        except ClientError as e:
            logger.exception(f"Error: {e}")
            raise
    else:
        raise ValueError("Cannot upload None - no data retrieved")
