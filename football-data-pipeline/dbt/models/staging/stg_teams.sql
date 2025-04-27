-- dbt/models/staging/stg_teams.sql
{{ config(
    materialized='table',
    unique_key='team_id'
) }}

SELECT
    id as team_id,
    team_name,
    league
FROM {{ source('raw', 'teams') }}

-- dbt/models/marts/dim_team_season_stats.sql
{{ config(
    materialized='table',
    unique_key='team_season_id'
) }}

WITH match_stats AS (
    SELECT
        season,
        league,
        home_team_id as team_id,
        COUNT(*) as home_matches,
        SUM(CASE WHEN result = 'home_win' THEN 1 ELSE 0 END) as home_wins,
        SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) as home_draws,
        SUM(CASE WHEN result = 'away_win' THEN 1 ELSE 0 END) as home_losses,
        SUM(home_goals) as home_goals_for,
        SUM(away_goals) as home_goals_against,
        SUM(home_xg) as home_xg_for,
        SUM(away_xg) as home_xg_against
    FROM {{ ref('fact_matches') }}
    GROUP BY season, league, home_team_id
    
    UNION ALL
    
    SELECT
        season,
        league,
        away_team_id as team_id,
        COUNT(*) as away_matches,
        SUM(CASE WHEN result = 'away_win' THEN 1 ELSE 0 END) as away_wins,
        SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) as away_draws,
        SUM(CASE WHEN result = 'home_win' THEN 1 ELSE 0 END) as away_losses,
        SUM(away_goals) as away_goals_for,
        SUM(home_goals) as away_goals_against,
        SUM(away_xg) as away_xg_for,
        SUM(home_xg) as away_xg_against
    FROM {{ ref('fact_matches') }}
    GROUP BY season, league, away_team_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['t.team_id', 'ms.season']) }} as team_season_id,
    t.team_id,
    t.team_name,
    ms.season,
    ms.league,
    SUM(ms.home_matches + ms.away_matches) as matches_played,
    SUM(ms.home_wins + ms.away_wins) as wins,
    SUM(ms.home_draws + ms.away_draws) as draws,
    SUM(ms.home_losses + ms.away_losses) as losses,
    SUM(ms.home_goals_for + ms.away_goals_for) as goals_for,
    SUM(ms.home_goals_against + ms.away_goals_against) as goals_against,
    SUM(ms.home_wins + ms.away_wins) * 3 + SUM(ms.home_draws + ms.away_draws) as points,
    SUM(ms.home_xg_for + ms.away_xg_for) as xg_for,
    SUM(ms.home_xg_against + ms.away_xg_against) as xg_against
FROM match_stats ms
JOIN {{ ref('stg_teams') }} t ON ms.team_id = t.team_id
GROUP BY t.team_id, t.team_name, ms.season, ms.league