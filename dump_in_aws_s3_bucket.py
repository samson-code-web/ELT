from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError, EndpointConnectionError, ProfileNotFound, ParamValidationError,ConnectionClosedError
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
            client = boto3.client("s3")
            logger.info(f"Sending file to bucket")
            client.upload_fileobj(Fileobj=file, Bucket="elt-github-623955527676-ca-central-1-an", Key="elt/raw/github.parquet", Config=config)
            logger.info('done uploading file to bucket')

        except ProfileNotFound as e:
            logger.exception(f"AWS profile not found: {e}")
            raise

        except NoCredentialsError as e:
            logger.exception(f"No AWS credentials found: {e}")
            raise

        except PartialCredentialsError as e:
            logger.exception(f"Incomplete credentials: {e}")
            raise

        except EndpointConnectionError as e:
            logger.exception(f"Cannot connect to S3 endpoint: {e}")
            raise

        except ParamValidationError as e:
            logger.exception(f"Bad parameters to S3: {e}")
            raise

        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.exception(f"S3 ClientError [{error_code}]: {e}")
            raise

        except (ConnectionError, ConnectionClosedError, TimeoutError) as e:
            logger.exception(f"Network failed during upload: {e}")
            raise
    else:
        raise ValueError("Cannot upload None - no data retrieved")
