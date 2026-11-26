{{ config(materialized='table', alias='cleaned_github_event_table') }}

SELECT repo:id::NUMBER(38,0) as repo_id ,
       repo:name::STRING as name ,
       repo:url::STRING as url,
       public
FROM {{ source('elt_source' , 'RAW_GITHUB_EVENTS')}}
WHERE public = TRUE
