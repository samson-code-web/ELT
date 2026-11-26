from prefect_dbt import PrefectDbtRunner , PrefectDbtSettings
from dump_in_aws_s3_bucket import send_to_bucket
from extract_data import retrieve_data
from prefect import flow , task
from datetime import datetime
from pathlib import Path
import logging

from set_up_snowflake import setting_up_snowflake
logger = logging.getLogger("elt")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    file_path = Path(__file__).resolve().parent / "app.log"

    file_handler = logging.FileHandler(str(file_path), mode="a", encoding="utf-8")

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

@task(retries=3, retry_delay_seconds=10)
def get_file(initial_date , final_date):
    logger.info("extraction step has started")
    return retrieve_data(start_date=initial_date, end_date=final_date)

@task(retries=3, retry_delay_seconds=10)
def uploading_file(parquet_file_buffer):
    logger.info("uploading step has started")
    return send_to_bucket(file=parquet_file_buffer)

@task(retries=3, retry_delay_seconds=30)
def execute_dbt_pipeline(project_dir: Path, profiles_dir: Path) -> None:
    settings = PrefectDbtSettings(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )
    runner = PrefectDbtRunner(settings=settings, raise_on_failure=True)
    runner.invoke(["deps"])
    runner.invoke(["build"])

@flow(name='EXTRACTING-LOADING-FLOW')
def trigger_action() -> None:
    initial_date = datetime(2026, 1, 1, 0)
    final_date = datetime(2026, 1, 1,24)

    path = get_file(initial_date=initial_date, final_date=final_date)

    uploading_file(path)

    setting_up_snowflake()

@flow(name="DBT-TRANSFORMATION-FLOW", log_prints=True)
def dbt_orchestration_flow( project_dir: Path = Path(__file__).resolve().parent / 'elt_project', profiles_dir: Path = Path("/home/samson/.dbt/").resolve()) -> None:
    try:
        logger.info(f"Starting dbt build for project at {project_dir}...")
        execute_dbt_pipeline(project_dir=project_dir, profiles_dir=profiles_dir)
        logger.info("dbt build completed successfully.")
    except Exception as e:
        logger.error(e)

