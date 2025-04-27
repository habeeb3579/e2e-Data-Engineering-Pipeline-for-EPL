-- dbt/models/staging/stg_players.sql
{{ config(
    materialized='table',
    unique_key='player_id'
) }}

SELECT
    id as player_id,
    player_name,
    team_id,
    position,
    nationality
FROM {{ source('raw', 'players') }}

-- dbt/models/staging/stg_shots.sql
{{ config(
    materialized='table'
) }}

SELECT
    id as shot_id,
    match_id,
    player_id,
    minute,
    result,
    x_coord,
    y_coord,
    xg,
    assisted_by,
    CAST(is_goal as BOOLEAN) as is_goal
FROM {{ source('raw', 'shots') }}

-- dbt/models/marts/fact_player_performance.sql
{{ config(
    materialized='table',
    unique_key='performance_id',
    partition_by={
      "field": "match_date",
      "data_type": "timestamp",
      "granularity": "day"
    } if target.type == 'bigquery' else none
) }}

WITH player_match_shots AS (
    SELECT
        s.match_id,
        s.player_id,
        m.match_date,
        COUNT(*) as shots,
        SUM(CASE WHEN s.is_goal THEN 1 ELSE 0 END) as goals,
        SUM(CASE WHEN s.assisted_by IS NOT NULL THEN 1 ELSE 0 END) as assists_made,
        SUM(s.xg) as xg
    FROM {{ ref('stg_shots') }} s
    JOIN {{ ref('stg_matches') }} m ON s.match_id = m.match_id
    GROUP BY s.match_id, s.player_id, m.match_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['pms.match_id', 'pms.player_id']) }} as performance_id,
    pms.match_id,
    pms.player_id,
    p.player_name,
    p.team_id,
    t.team_name,
    m.league,
    m.season,
    pms.match_date,
    pms.goals,
    pms.assists_made as assists,
    pms.shots,
    pms.xg,
    pms.goals - pms.xg as xg_diff
FROM player_match_shots pms
JOIN {{ ref('stg_players') }} p ON pms.player_id = p.player_id
JOIN {{ ref('stg_teams') }} t ON p.team_id = t.team_id
JOIN {{ ref('stg_matches') }} m ON pms.match_id = m.match_id