-- dbt/models/staging/stg_matches.sql
{{ config(
    materialized='table',
    unique_key='match_id',
    partition_by={
      "field": "match_date",
      "data_type": "timestamp",
      "granularity": "day"
    } if target.type == 'bigquery' else none
) }}

SELECT
    id as match_id,
    home_team,
    away_team,
    league,
    season,
    CAST(date as TIMESTAMP) as match_date,
    CAST(home_goals as INT) as home_goals,
    CAST(away_goals as INT) as away_goals,
    CAST(home_xg as FLOAT) as home_xg,
    CAST(away_xg as FLOAT) as away_xg
FROM {{ source('raw', 'matches') }}

-- dbt/models/marts/fact_matches.sql
{{ config(
    materialized='table',
    unique_key='match_id',
    partition_by={
      "field": "match_date",
      "data_type": "timestamp",
      "granularity": "day"
    } if target.type == 'bigquery' else none
) }}

WITH home_teams AS (
    SELECT team_id, team_name
    FROM {{ ref('stg_teams') }}
),
away_teams AS (
    SELECT team_id, team_name
    FROM {{ ref('stg_teams') }}
)

SELECT
    m.match_id,
    ht.team_id as home_team_id,
    at.team_id as away_team_id,
    m.league,
    m.season,
    m.match_date,
    CASE
        WHEN m.home_goals > m.away_goals THEN 'home_win'
        WHEN m.home_goals < m.away_goals THEN 'away_win'
        ELSE 'draw'
    END as result,
    m.home_goals,
    m.away_goals,
    m.home_xg,
    m.away_xg,
    (m.home_xg - m.home_goals) as home_xg_diff,
    (m.away_xg - m.away_goals) as away_xg_diff
FROM {{ ref('stg_matches') }} m
LEFT JOIN home_teams ht ON m.home_team = ht.team_name
LEFT JOIN away_teams at ON m.away_team = at.team_name