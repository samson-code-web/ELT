import logging
from pathlib import Path
import snowflake.connector as sf
from dbt.config.profile import Profile
from dbt.config.renderer import ProfileRenderer
from dbt.clients.yaml_helper import load_yaml_text

from dbt.flags import set_from_args
from argparse import Namespace

logger = logging.getLogger(f'elt{__file__}')

set_from_args(Namespace(), None)

profiles_path = Path.home() / ".dbt" / "profiles.yml"

raw_profiles = load_yaml_text(profiles_path.read_text(encoding="utf-8"))

profile = Profile.from_raw_profiles(
    raw_profiles=raw_profiles,
    profile_name="elt_project",
    renderer=ProfileRenderer()
)

creds = profile.credentials

def setting_up_snowflake():
    with sf.connect(
    user=creds.user,
    password=creds.password,
    account=creds.account,
    warehouse=creds.warehouse,
    database=creds.database,
    schema=creds.schema,
    role=creds.role,
    authenticator=creds.authenticator,
    enable_server_session_keep_alive_auto_detection=False) as conn :

        with conn.cursor() as cursor:
            logger.info("trying to use the right database...")
            cursor.execute("USE DATABASE TEST_DATABASE;")
            logger.info("database successfully selected")

            logger.info("trying to use the right schema...")
            cursor.execute("USE SCHEMA RAW;")
            logger.info("schema successfully selected")

            logger.info("trying to create the RAW_GITHUB_EVENTS table...")
            cursor.execute('''
                CREATE OR REPLACE TABLE RAW_GITHUB_EVENTS (
                    id VARCHAR,
                    type VARCHAR,
                    actor VARIANT,
                    repo VARIANT,
                    payload VARIANT,
                    public BOOLEAN,
                    created_at TIMESTAMP_NTZ,
                    org VARIANT);'''
            )
            logger.info("RAW_GITHUB_EVENTS table successfully created")

            logger.info("trying to upload the parquet file from s3_bucket...")
            cursor.execute("""
                        COPY INTO RAW_GITHUB_EVENTS
                        FROM @S3_STAGE/raw/github.parquet
                        FILE_FORMAT = (TYPE = 'PARQUET')
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;""")

            logger.info("parquet file successfully uploaded")
