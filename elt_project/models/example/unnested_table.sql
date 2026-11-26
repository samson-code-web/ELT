{{ config( materialized='table', alias='cleaned_github_event_table') }}

WITH ranked_events AS (
    SELECT
        repo:id::NUMBER(38,0) AS repo_id,
        repo:name::STRING AS name,
        repo:url::STRING AS url,
        created_at,
        public,
        ROW_NUMBER() OVER (
            PARTITION BY repo:id::NUMBER(38,0)
            ORDER BY created_at DESC
        ) AS rn
    FROM {{ source('elt_source', 'RAW_GITHUB_EVENTS') }}
    WHERE public = TRUE
)

SELECT
    repo_id,
    name,
    url,
    created_at,
    public
FROM ranked_events
WHERE rn = 1
