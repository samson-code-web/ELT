
---
# GitHub Events ELT Pipeline

An end-to-end ELT pipeline that extracts GitHub public events from GH Archive, loads them into AWS S3 and Snowflake, and transforms them with dbt — orchestrated by Prefect.

Built to run automatically every Monday at 8:00 AM (America/Toronto).

## Architecture

GH Archive (JSON.gz) 
    → Python + Polars (Extract)
    → AWS S3 (Raw Parquet)
    → Snowflake (RAW_GITHUB_EVENTS)
    → dbt (Transform)
    → Snowflake (CLEANED_GITHUB_EVENT_TABLE)

**Orchestration:** Prefect 3.9.5
**Flow:** `EXTRACTING-LOADING-FLOW` → `DBT-TRANSFORMATION-FLOW` (as subflow)

### Pipeline Steps

1. **Extract (`extract_data.py`)**
        - Downloads hourly GH Archive files for a given date range via `httpx`
        - Streams + decompresses `.json.gz` files in memory
        - Parses with Polars `read_ndjson`
        - Concatenates all hours into one DataFrame
        - Writes to Parquet (Snappy compression) in a `BytesIO` buffer

2. **Load to S3 (`dump_in_aws_s3_bucket.py`)**
        - Uploads Parquet buffer to S3 using `boto3` multipart upload
        - Bucket: `elt-github-623955527676-ca-central-1-an`
        - Key: `elt/raw/github.parquet`
        - TransferConfig: 1MB chunk size, 16MB multipart threshold, 5 concurrent threads

3. **Load to Snowflake (`set_up_snowflake.py`)**
        - Connects to Snowflake using credentials from `~/.dbt/profiles.yml`
        - Uses database `TEST_DATABASE`, schema `RAW`
        - Creates table `RAW_GITHUB_EVENTS` if it doesn't exist
        - Executes `COPY INTO` from S3 Stage `@S3_STAGE/raw/github.parquet`
        - Uses `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` for Parquet mapping

4. **Transform with dbt (`elt_project/`)**

**Model:** `unnested_table` → `CLEANED_GITHUB_EVENT_TABLE`
**Materialization:** `table`

Unnests the `repo` VARIANT column into:
- `repo_id` — `repo:id::NUMBER(38,0)`
- `name` — `repo:name::STRING`
- `url` — `repo:url::STRING`

**Final columns:**
- `repo_id`
- `name`
- `url`
- `created_at`
- `public`

**Transformations:**
- Filters to `WHERE public = TRUE`
- Deduplicates to 1 row per repo using `ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY created_at DESC)` and keeps only `rn = 1` (latest event per repo)

**Source:** `elt_source.RAW_GITHUB_EVENTS`


## Tech Stack

| Layer | Tool |
| :--- | :--- |
| Orchestration | Prefect 3 |
| Extraction | Python, httpx, Polars |
| Storage | AWS S3, Parquet |
| Warehouse | Snowflake |
| Transformation | dbt |
| Language | Python 3.12+ |

## Project Structure

ELT/
├── http://main.py # Prefect flows + tasks (entrypoint)
├── extract_data.py # GH Archive → Parquet
├── dump_in_aws_s3_bucket.py # Parquet → S3
├── set_up_snowflake.py # S3 → Snowflake COPY INTO
├── http://prefect.yaml # Deployment + schedule
├── elt_project/
│ ├── dbt_project.yml
│ ├── models/
│ │ ├── unnested_table.sql
│ │ └── http://schema.yml
│ └── http://profiles.yml (in ~/.dbt/)
├── .prefectignore
└── http://README.md

## Setup

### 1. Prerequisites

- Python 3.12+
- AWS CLI configured with profile `samson-elt`
- Snowflake account with access to `TEST_DATABASE`
- dbt + Prefect installed
- S3 Stage `@S3_STAGE` already created in Snowflake

### 2. Install Dependencies

```bash
pip install prefect prefect-dbt polars httpx boto3 snowflake-connector-python dbt-snowflake
### 3. Configure dbt Profile

Create `~/.dbt/profiles.yml`:
elt_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <YOUR_ACCOUNT>
      user: <YOUR_USER>
      password: <YOUR_PASSWORD>
      role: <YOUR_ROLE>
      database: TEST_DATABASE
      warehouse: <YOUR_WAREHOUSE>
      schema: RAW
      threads: 4
### 4. Configure AWS
aws configure --profile samson-elt
## Running Locally

### Run the full pipeline once
python main.py
Or run via Prefect:
prefect run --flow main_flow
This will run:
1. Extract
2. Load to S3
3. Load to Snowflake
4. Run dbt `deps` + `build` as a subflow

### Run only dbt
cd elt_project
dbt build
## Deployment (EC2 + Prefect)

The pipeline is deployed as a Prefect Deployment with a work pool.

*`prefect.yaml`:*
name: ELT
prefect-version: 3.9.5

pull:
- prefect.deployments.steps.set_working_directory:
    directory: /home/ubuntu/ELT

deployments:
    - name: elt-flow
    entrypoint: main.py:main_flow
    work_pool:
      name: elt-pool
    schedules:
            - cron: "0 8 * * 1"
        timezone: "America/Toronto"
        active: true
*Deploy:*
prefect deploy --all
*Start worker on EC2:*
prefect worker start --pool elt-pool
Schedule: *Every Monday at 8:00 AM America/Toronto*

## Error Handling & Reliability

- All Prefect tasks use `retries=3` with backoff
- `logger.exception()` + `raise` pattern: errors are logged AND propagated to Prefect, so failures are not silently swallowed
- Graceful early exit: if no data is retrieved from GH Archive, the flow logs and returns instead of trying to upload an empty file
- `CREATE TABLE IF NOT EXISTS` prevents data loss on re-runs
- dbt runs as a *subflow*, not a separate scheduled flow — if Extract/Load fails, dbt will NOT run on stale data

## Logging

Logs are written to `app.log` in the project root:
2026-01-01 08:00:00 - INFO - extraction step has started
2026-01-01 08:00:05 - INFO - Downloading: https://data.gharchive.org/2026-01-01-0.json.gz
2026-01-01 08:00:10 - INFO - done uploading file to bucket
2026-01-01 08:00:12 - INFO - parquet file successfully uploaded
Prefect UI also shows task-level logs and flow run history.

## dbt Models

### `unnested_table` → `CLEANED_GITHUB_EVENT_TABLE`

*Source:* `TEST_DATABASE.RAW.RAW_GITHUB_EVENTS`
SELECT
    repo:id::NUMBER(38,0) AS repo_id,
    repo:name::STRING AS name,
    repo:url::STRING AS url,
    public
FROM {{ source('elt_source', 'RAW_GITHUB_EVENTS') }}
WHERE public = TRUE
Materialized as a `table`.

## Future Improvements

- [ ] Parameterize date range (currently hardcoded to 2026-01-01) to use `{{ scheduled_start_time }}` or Prefect parameters
- [ ] Add data quality tests with dbt tests (`unique`, `not_null`)
- [ ] Add S3 partitioning by date: `elt/raw/year=2026/month=01/day=01/github.parquet`
- [ ] Add Slack / email alerting on Prefect flow failure
- [ ] Add incremental dbt model instead of full refresh

## Author

Samson — ELT / Data Engineering Project

---

That's it. Paste that in, commit it.

Pro tip: GitHub will render that architecture diagram and the tables really clean. Recruiters love a README that explains *why* you made decisions, not just *what* you did — that's why I added the Error Handling section. That's your 9/10 code talking.

Want me to also write a short LinkedIn post version of this project so you can post it?
