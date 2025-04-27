import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.config import Config
from src.extraction.understat.scraper import UnderstatScraper

# Configuration
@st.cache_resource
def get_config():
    config_path = os.getenv("CONFIG_PATH", "../config/config.yaml")
    return Config(config_path)

config = get_config()

# Set up database connection based on storage type
@st.cache_resource
def get_db_connection():
    storage_type = config.get("storage.type", "postgres")
    
    if storage_type == "postgres":
        import psycopg2
        conn = psycopg2.connect(
            host=config.get("postgres.host", "localhost"),
            port=config.get("postgres.port", 5432),
            database=config.get("postgres.database", "football"),
            user=config.get("postgres.user", "postgres"),
            password=config.get("postgres.password", "")
        )
        return conn
    elif storage_type == "gcp":
        from google.cloud import bigquery
        from google.oauth2 import service_account
        
        credentials_path = config.get("gcp.credentials_path")
        project_id = config.get("gcp.project_id")
        
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            client = bigquery.Client(project=project_id)
            
        return client
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

# Data loading functions
@st.cache_data(ttl=3600)
def load_matches(league=None, season=None, limit=100):
    """Load match data from database."""
    storage_type = config.get("storage.type", "postgres")
    
    if storage_type == "postgres":
        conn = get_db_connection()
        query = """
            SELECT id, home_team, away_team, home_goals, away_goals, date, 
                  league, season, is_finished, home_xg, away_xg
            FROM football.matches
            WHERE 1=1
        """
        params = []
        
        if league:
            query += " AND league = %s"
            params.append(league)
        if season:
            query += " AND season = %s"
            params.append(season)
            
        query += " ORDER BY date DESC LIMIT %s"
        params.append(limit)
        
        return pd.read_sql(query, conn, params=params)
    elif storage_type == "gcp":
        client = get_db_connection()
        dataset = config.get("gcp.dataset", "football")
        
        query = f"""
            SELECT id, home_team, away_team, home_goals, away_goals, date, 
                  league, season, is_finished, home_xg, away_xg
            FROM `{dataset}.matches`
            WHERE 1=1
        """
        
        if league:
            query += f" AND league = '{league}'"
        if season:
            query += f" AND season = '{season}'"
            
        query += f" ORDER BY date DESC LIMIT {limit}"
        
        return client.query(query).to_dataframe()
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

@st.cache_data(ttl=3600)
def load_player_stats(league=None, season=None, limit=100):
    """Load player stats data from database."""
    storage_type = config.get("storage.type", "postgres")
    
    if storage_type == "postgres":
        conn = get_db_connection()
        query = """
            SELECT player_id, player_name, team, position, 
                  SUM(goals) as total_goals, 
                  SUM(assists) as total_assists,
                  SUM(xg) as total_xg,
                  SUM(xa) as total_xa,
                  SUM(shots) as total_shots,
                  SUM(key_passes) as total_key_passes,
                  COUNT(DISTINCT match_id) as matches_played
            FROM football.player_stats
            WHERE 1=1
        """
        params = []
        
        if league:
            query += " AND league = %s"
            params.append(league)
        if season:
            query += " AND season = %s"
            params.append(season)
            
        query += " GROUP BY player_id, player_name, team, position ORDER BY total_goals DESC LIMIT %s"
        params.append(limit)
        
        return pd.read_sql(query, conn, params=params)
    elif storage_type == "gcp":
        client = get_db_connection()
        dataset = config.get("gcp.dataset", "football")
        
        query = f"""
            SELECT player_id, player_name, team, position, 
                  SUM(goals) as total_goals, 
                  SUM(assists) as total_assists,
                  SUM(xg) as total_xg,
                  SUM(xa) as total_xa,
                  SUM(shots) as total_shots,
                  SUM(key_passes) as total_key_passes,
                  COUNT(DISTINCT match_id) as matches_played
            FROM `{dataset}.player_stats`
            WHERE 1=1
        """
        
        if league:
            query += f" AND league = '{league}'"
        if season:
            query += f" AND season = '{season}'"
            
        query += f" GROUP BY player_id, player_name, team, position ORDER BY total_goals DESC LIMIT {limit}"
        
        return client.query(query).to_dataframe()
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

@st.cache_data(ttl=3600)
def load_team_stats(league=None, season=None):
    """Load team stats data from database."""
    storage_type = config.get("storage.type", "postgres")
    
    if storage_type == "postgres":
        conn = get_db_connection()
        query = """
            WITH team_matches AS (
                SELECT 
                    home_team as team,
                    home_goals as goals_for,
                    away_goals as goals_against,
                    home_xg as xg_for,
                    away_xg as xg_against,
                    CASE 
                        WHEN home_goals > away_goals THEN 3
                        WHEN home_goals = away_goals THEN 1
                        ELSE 0
                    END as points,
                    CASE 
                        WHEN home_goals > away_goals THEN 1
                        ELSE 0
                    END as wins,
                    CASE
                        WHEN home_goals = away_goals THEN 1
                        ELSE 0
                    END as draws,
                    CASE
                        WHEN home_goals < away_goals THEN 1
                        ELSE 0
                    END as losses,
                    1 as played,
                    league,
                    season
                FROM football.matches
                WHERE is_finished = TRUE
                
                UNION ALL
                
                SELECT 
                    away_team as team,
                    away_goals as goals_for,
                    home_goals as goals_against,
                    away_xg as xg_for,
                    home_xg as xg_against,
                    CASE 
                        WHEN away_goals > home_goals THEN 3
                        WHEN away_goals = home_goals THEN 1
                        ELSE 0
                    END as points,
                    CASE 
                        WHEN away_goals > home_goals THEN 1
                        ELSE 0
                    END as wins,
                    CASE
                        WHEN away_goals = home_goals THEN 1
                        ELSE 0
                    END as draws,
                    CASE
                        WHEN away_goals < home_goals THEN 1
                        ELSE 0
                    END as losses,
                    1 as played,
                    league,
                    season
                FROM football.matches
                WHERE is_finished = TRUE
            )
            SELECT 
                team,
                SUM(points) as total_points,
                SUM(played) as matches_played,
                SUM(wins) as wins,
                SUM(draws) as draws,
                SUM(losses) as losses,
                SUM(goals_for) as goals_for,
                SUM(goals_against) as goals_against,
                SUM(goals_for) - SUM(goals_against) as goal_diff,
                SUM(xg_for) as xg_for,
                SUM(xg_against) as xg_against,
                SUM(xg_for) - SUM(xg_against) as xg_diff,
                league,
                season
            FROM team_matches
            WHERE 1=1
        """
        params = []
        
        if league:
            query += " AND league = %s"
            params.append(league)
        if season:
            query += " AND season = %s"
            params.append(season)
            
        query += " GROUP BY team, league, season ORDER BY total_points DESC, goal_diff DESC"
        
        return pd.read_sql(query, conn, params=params)
    elif storage_type == "gcp":
        client = get_db_connection()
        dataset = config.get("gcp.dataset", "football")
        
        query = f"""
            WITH team_matches AS (
                SELECT 
                    home_team as team,
                    home_goals as goals_for,
                    away_goals as goals_against,
                    home_xg as xg_for,
                    away_xg as xg_against,
                    CASE 
                        WHEN home_goals > away_goals THEN 3
                        WHEN home_goals = away_goals THEN 1
                        ELSE 0
                    END as points,
                    CASE 
                        WHEN home_goals > away_goals THEN 1
                        ELSE 0
                    END as wins,
                    CASE
                        WHEN home_goals = away_goals THEN 1
                        ELSE 0
                    END as draws,
                    CASE
                        WHEN home_goals < away_goals THEN 1
                        ELSE 0
                    END as losses,
                    1 as played,
                    league,
                    season
                FROM `{dataset}.matches`
                WHERE is_finished = TRUE
                
                UNION ALL
                
                SELECT 
                    away_team as team,
                    away_goals as goals_for,
                    home_goals as goals_against,
                    away_xg as xg_for,
                    home_xg as xg_against,
                    CASE 
                        WHEN away_goals > home_goals THEN 3
                        WHEN away_goals = home_goals THEN 1
                        ELSE 0
                    END as points,
                    CASE 
                        WHEN away_goals > home_goals THEN 1
                        ELSE 0
                    END as wins,
                    CASE
                        WHEN away_goals = home_goals THEN 1
                        ELSE 0
                    END as draws,
                    CASE
                        WHEN away_goals < home_goals THEN 1
                        ELSE 0
                    END as losses,
                    1 as played,
                    league,
                    season
                FROM `{dataset}.matches`
                WHERE is_finished = TRUE
            )
            SELECT 
                team,
                SUM(points) as total_points,
                SUM(played) as matches_played,
                SUM(wins) as wins,
                SUM(draws) as draws,
                SUM(losses) as losses,
                SUM(goals_for) as goals_for,
                SUM(goals_against) as goals_against,
                SUM(goals_for) - SUM(goals_against) as goal_diff,
                SUM(xg_for) as xg_for,
                SUM(xg_against) as xg_against,
                SUM(xg_for) - SUM(xg_against) as xg_diff,
                league,
                season
            FROM team_matches
            WHERE 1=1
        """
        
        if league:
            query += f" AND league = '{league}'"
        if season:
            query += f" AND season = '{season}'"
            
        query += " GROUP BY team, league, season ORDER BY total_points DESC, goal_diff DESC"
        
        return client.query(query).to_dataframe()
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

@st.cache_data(ttl=3600)
def load_shot_heatmap_data(team=None, league=None, season=None):
    """Load shot data for heatmap visualization."""
    storage_type = config.get("storage.type", "postgres")
    
    if storage_type == "postgres":
        conn = get_db_connection()
        query = """
            SELECT x, y, xg, result, situation, team
            FROM football.shots
            WHERE 1=1
        """
        params = []
        
        if team:
            query += " AND team = %s"
            params.append(team)
        if league:
            query += " AND league = %s"
            params.append(league)
        if season:
            query += " AND season = %s"
            params.append(season)
            
        return pd.read_sql(query, conn, params=params)
    elif storage_type == "gcp":
        client = get_db_connection()
        dataset = config.get("gcp.dataset", "football")
        
        query = f"""
            SELECT x, y, xg, result, situation, team
            FROM `{dataset}.shots`
            WHERE 1=1
        """
        
        if team:
            query += f" AND team = '{team}'"
        if league:
            query += f" AND league = '{league}'"
        if season:
            query += f" AND season = '{season}'"
            
        return client.query(query).to_dataframe()
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

# Get available leagues and seasons
@st.cache_data(ttl=3600)
def get_leagues_and_seasons():
    """Get available leagues and seasons from database."""
    storage_type = config.get("storage.type", "postgres")
    
    if storage_type == "postgres":
        conn = get_db_connection()
        query = """
            SELECT DISTINCT league, season
            FROM football.matches
            ORDER BY league, season DESC
        """
        df = pd.read_sql(query, conn)
    elif storage_type == "gcp":
        client = get_db_connection()
        dataset = config.get("gcp.dataset", "football")
        
        query = f"""
            SELECT DISTINCT league, season
            FROM `{dataset}.matches`
            ORDER BY league, season DESC
        """
        df = client.query(query).to_dataframe()
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")
        
    leagues = df['league'].unique().tolist() if not df.empty else []
    seasons = df['season'].unique().tolist() if not df.empty else []
    
    return leagues, seasons

# Get available teams
@st.cache_data(ttl=3600)
def get_teams(league=None, season=None):
    """Get available teams from database."""
    storage_type = config.get("storage.type", "postgres")
    
    if storage_type == "postgres":
        conn = get_db_connection()
        query = """
            SELECT DISTINCT team
            FROM (
                SELECT home_team as team FROM football.matches
                UNION
                SELECT away_team as team FROM football.matches
            ) teams
            WHERE 1=1
        """
        params = []
        
        if league:
            query += " AND league = %s"
            params.append(league)
        if season:
            query += " AND season = %s"
            params.append(season)
            
        query += " ORDER BY team"
        
        df = pd.read_sql(query, conn, params=params)
    elif storage_type == "gcp":
        client = get_db_connection()
        dataset = config.get("gcp.dataset", "football")
        
        query = f"""
            SELECT DISTINCT team
            FROM (
                SELECT home_team as team FROM `{dataset}.matches`
                UNION
                SELECT away_team as team FROM `{dataset}.matches`
            ) teams
            WHERE 1=1
        """
        
        if league:
            query += f" AND league = '{league}'"
        if season:
            query += f" AND season = '{season}'"
            
        query += " ORDER BY team"
        
        df = client.query(query).to_dataframe()
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")
        
    return df['team'].tolist() if not df.empty else []

# Streamlit App
st.set_page_config(page_title="Football Data Dashboard", page_icon="⚽", layout="wide")

# Sidebar
st.sidebar.title("⚽ Football Data Dashboard")

# Get available leagues and seasons
leagues, seasons = get_leagues_and_seasons()

# Filters
selected_league = st.sidebar.selectbox("Select League", ["All"] + leagues, index=0)
league_filter = selected_league if selected_league != "All" else None

selected_season = st.sidebar.selectbox("Select Season", ["All"] + seasons, index=0)
season_filter = selected_season if selected_season != "All" else None

# Navigation
page = st.sidebar.radio("Navigation", ["League Table", "Top Players", "Team Analysis", "Match Results", "Shot Analysis"])

st.sidebar.markdown("---")
st.sidebar.info(
    "This dashboard shows football statistics from UnderStat data. "
    "Select a league and season to filter the data, and use the navigation to explore different aspects."
)

# Main content
if page == "League Table":
    st.title("League Table")
    
    if not leagues:
        st.warning("No data available. Please ensure the data pipeline is running and data is being collected.")
    else:
        team_stats = load_team_stats(league_filter, season_filter)
        
        if team_stats.empty:
            st.warning("No team statistics found for the selected filters.")
        else:
            # Display league table
            st.dataframe(
                team_stats[['team', 'total_points', 'matches_played', 'wins', 'draws', 'losses', 
                            'goals_for', 'goals_against', 'goal_diff']],
                use_container_width=True,
                hide_index=True
            )
            
            # Points chart
            st.subheader("Points Distribution")
            fig_points = px.bar(
                team_stats, 
                x='team', 
                y='total_points',
                color='team',
                labels={'team': 'Team', 'total_points': 'Points'},
                title="Total Points by Team"
            )
            st.plotly_chart(fig_points, use_container_width=True)
            
            # Expected Goals vs Actual Goals
            st.subheader("Expected Goals vs Actual Goals")
            xg_comparison = pd.melt(
                team_stats, 
                id_vars=['team'], 
                value_vars=['goals_for', 'xg_for', 'goals_against', 'xg_against'],
                var_name='metric', 
                value_name='value'
            )
            
            fig_xg = px.bar(
                xg_comparison, 
                x='team', 
                y='value', 
                color='metric', 
                barmode='group',
                labels={'team': 'Team', 'value': 'Goals', 'metric': 'Metric'},
                title="Expected vs Actual Goals by Team",
                color_discrete_map={
                    'goals_for': '#3366CC', 
                    'xg_for': '#6699FF',
                    'goals_against': '#CC3366', 
                    'xg_against': '#FF6699'
                }
            )
            st.plotly_chart(fig_xg, use_container_width=True)
            
elif page == "Top Players":
    st.title("Top Players")
    
    if not leagues:
        st.warning("No data available. Please ensure the data pipeline is running and data is being collected.")
    else:
        player_stats = load_player_stats(league_filter, season_filter)
        
        if player_stats.empty:
            st.warning("No player statistics found for the selected filters.")
        else:
            # Tabs for different player metrics
            tab1, tab2, tab3, tab4 = st.tabs(["Goals", "Assists", "Expected Goals", "Expected Assists"])
            
            with tab1:
                st.subheader("Top Goal Scorers")
                
                # Goals leaderboard
                top_scorers = player_stats.sort_values('total_goals', ascending=False).head(20)
                
                # Display top scorers table
                st.dataframe(
                    top_scorers[['player_name', 'team', 'position', 'total_goals', 'matches_played']].rename(
                        columns={
                            'player_name': 'Player',
                            'team': 'Team',
                            'position': 'Position',
                            'total_goals': 'Goals',
                            'matches_played': 'Matches'
                        }
                    ),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Goals chart
                fig_goals = px.bar(
                    top_scorers.head(10), 
                    x='player_name', 
                    y='total_goals',
                    color='team',
                    text='total_goals',
                    labels={'player_name': 'Player', 'total_goals': 'Goals', 'team': 'Team'},
                    title="Top 10 Goal Scorers"
                )
                fig_goals.update_traces(texttemplate='%{text:.0f}', textposition='outside')
                st.plotly_chart(fig_goals, use_container_width=True)
                
                # Goals per match
                top_scorers['goals_per_match'] = top_scorers['total_goals'] / top_scorers['matches_played']
                
                fig_goals_per_match = px.bar(
                    top_scorers.head(10), 
                    x='player_name', 
                    y='goals_per_match',
                    color='team',
                    text='goals_per_match',
                    labels={'player_name': 'Player', 'goals_per_match': 'Goals per Match', 'team': 'Team'},
                    title="Goals per Match - Top 10 Scorers"
                )
                fig_goals_per_match.update_traces(texttemplate='%{text:.2f}', textposition='outside')
                st.plotly_chart(fig_goals_per_match, use_container_width=True)
                
            with tab2:
                st.subheader("Top Assisters")
                
                # Assists leaderboard
                top_assisters = player_stats.sort_values('total_assists', ascending=False).head(20)
                
                # Display top assisters table
                st.dataframe(
                    top_assisters[['player_name', 'team', 'position', 'total_assists', 'matches_played']].rename(
                        columns={
                            'player_name': 'Player',
                            'team': 'Team',
                            'position': 'Position',
                            'total_assists': 'Assists',
                            'matches_played': 'Matches'
                        }
                    ),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Assists chart
                fig_assists = px.bar(
                    top_assisters.head(10), 
                    x='player_name', 
                    y='total_assists',
                    color='team',
                    text='total_assists',
                    labels={'player_name': 'Player', 'total_assists': 'Assists', 'team': 'Team'},
                    title="Top 10 Assisters"
                )
                fig_assists.update_traces(texttemplate='%{text:.0f}', textposition='outside')
                st.plotly_chart(fig_assists, use_container_width=True)
                
            with tab3:
                st.subheader("Expected Goals (xG) Leaders")
                
                # xG leaderboard
                top_xg = player_stats.sort_values('total_xg', ascending=False).head(20)
                
                # Display top xG table
                st.dataframe(
                    top_xg[['player_name', 'team', 'position', 'total_xg', 'total_goals', 'matches_played']].rename(
                        columns={
                            'player_name': 'Player',
                            'team': 'Team',
                            'position': 'Position',
                            'total_xg': 'xG',
                            'total_goals': 'Goals',
                            'matches_played': 'Matches'
                        }
                    ),
                    use_container_width=True,
                    hide_index=True
                )
                
                # xG vs Goals chart
                top_xg_comparison = top_xg.head(10)
                
                fig_xg_vs_goals = go.Figure()
                fig_xg_vs_goals.add_trace(go.Bar(
                    x=top_xg_comparison['player_name'],
                    y=top_xg_comparison['total_xg'],
                    name='Expected Goals (xG)',
                    marker_color='#6699FF'
                ))
                fig_xg_vs_goals.add_trace(go.Bar(
                    x=top_xg_comparison['player_name'],
                    y=top_xg_comparison['total_goals'],
                    name='Actual Goals',
                    marker_color='#3366CC'
                ))
                
                fig_xg_vs_goals.update_layout(
                    title="Expected vs Actual Goals - Top 10 xG Leaders",
                    xaxis_title="Player",
                    yaxis_title="Goals",
                    barmode='group'
                )
                
                st.plotly_chart(fig_xg_vs_goals, use_container_width=True)
                
                # xG efficiency
                top_xg_comparison['xg_efficiency'] = (top_xg_comparison['total_goals'] - top_xg_comparison['total_xg']) 
                top_xg_comparison = top_xg_comparison.sort_values('xg_efficiency', ascending=False)
                
                fig_xg_efficiency = px.bar(
                    top_xg_comparison,
                    x='player_name',
                    y='xg_efficiency',
                    color='team',
                    text='xg_efficiency',
                    labels={'player_name': 'Player', 'xg_efficiency': 'Goals - xG (Efficiency)', 'team': 'Team'},
                    title="Player Finishing Efficiency (Goals - xG)"
                )
                fig_xg_efficiency.update_traces(texttemplate='%{text:.2f}', textposition='outside')
                st.plotly_chart(fig_xg_efficiency, use_container_width=True)
                
            with tab4:
                st.subheader("Expected Assists (xA) Leaders")
                
                # xA leaderboard
                top_xa = player_stats.sort_values('total_xa', ascending=False).head(20)
                
                # Display top xA table
                st.dataframe(
                    top_xa[['player_name', 'team', 'position', 'total_xa', 'total_assists', 'total_key_passes', 'matches_played']].rename(
                        columns={
                            'player_name': 'Player',
                            'team': 'Team',
                            'position': 'Position',
                            'total_xa': 'xA',
                            'total_assists': 'Assists',
                            'total_key_passes': 'Key Passes',
                            'matches_played': 'Matches'
                        }
                    ),
                    use_container_width=True,
                    hide_index=True
                )
                
                # xA vs Assists chart
                top_xa_comparison = top_xa.head(10)
                
                fig_xa_vs_assists = go.Figure()
                fig_xa_vs_assists.add_trace(go.Bar(
                    x=top_xa_comparison['player_name'],
                    y=top_xa_comparison['total_xa'],
                    name='Expected Assists (xA)',
                    marker_color='#FF9966'
                ))
                fig_xa_vs_assists.add_trace(go.Bar(
                    x=top_xa_comparison['player_name'],
                    y=top_xa_comparison['total_assists'],
                    name='Actual Assists',
                    marker_color='#FF6633'
                ))
                
                fig_xa_vs_assists.update_layout(
                    title="Expected vs Actual Assists - Top 10 xA Leaders",
                    xaxis_title="Player",
                    yaxis_title="Assists",
                    barmode='group'
                )
                
                st.plotly_chart(fig_xa_vs_assists, use_container_width=True)
                
                # Key passes to xA ratio
                top_xa_comparison['key_pass_to_xa_ratio'] = top_xa_comparison['total_xa'] / top_xa_comparison['total_key_passes']
                
                fig_key_pass_quality = px.bar(
                    top_xa_comparison,
                    x='player_name',
                    y='key_pass_to_xa_ratio',
                    color='team',
                    text='key_pass_to_xa_ratio',
                    labels={'player_name': 'Player', 'key_pass_to_xa_ratio': 'xA per Key Pass', 'team': 'Team'},
                    title="Pass Quality - xA per Key Pass"
                )
                fig_key_pass_quality.update_traces(texttemplate='%{text:.3f}', textposition='outside')
                st.plotly_chart(fig_key_pass_quality, use_container_width=True)
                
elif page == "Team Analysis":
    st.title("Team Analysis")
    
    if not leagues:
        st.warning("No data available. Please ensure the data pipeline is running and data is being collected.")
    else:
        # Get available teams based on filters
        teams = get_teams(league_filter, season_filter)
        
        if not teams:
            st.warning("No teams found for the selected filters.")
        else:
            # Team selection
            selected_team = st.selectbox("Select Team", teams)
            
            # Load team match data
            matches_df = load_matches(league_filter, season_filter, limit=1000)
            
            # Filter matches for selected team
            team_matches = matches_df[(matches_df['home_team'] == selected_team) | (matches_df['away_team'] == selected_team)]
            
            if team_matches.empty:
                st.warning(f"No match data found for {selected_team}.")
            else:
                # Process team match data
                team_matches['is_home'] = team_matches['home_team'] == selected_team
                team_matches['team_goals'] = team_matches.apply(lambda x: x['home_goals'] if x['is_home'] else x['away_goals'], axis=1)
                team_matches['opponent_goals'] = team_matches.apply(lambda x: x['away_goals'] if x['is_home'] else x['home_goals'], axis=1)
                team_matches['team_xg'] = team_matches.apply(lambda x: x['home_xg'] if x['is_home'] else x['away_xg'], axis=1)
                team_matches['opponent_xg'] = team_matches.apply(lambda x: x['away_xg'] if x['is_home'] else x['home_xg'], axis=1)
                team_matches['result'] = team_matches.apply(
                    lambda x: 'Win' if x['team_goals'] > x['opponent_goals'] else 
                             ('Draw' if x['team_goals'] == x['opponent_goals'] else 'Loss'), 
                    axis=1
                )
                team_matches['opponent'] = team_matches.apply(lambda x: x['away_team'] if x['is_home'] else x['home_team'], axis=1)
                
                # Key metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    matches_played = len(team_matches)
                    wins = len(team_matches[team_matches['result'] == 'Win'])
                    draws = len(team_matches[team_matches['result'] == 'Draw'])
                    losses = len(team_matches[team_matches['result'] == 'Loss'])
                    points = wins * 3 + draws
                    
                    st.metric("Matches Played", matches_played)
                    st.metric("Points", points)
                
                with col2:
                    st.metric("Wins", wins)
                    st.metric("Draws", draws)
                    st.metric("Losses", losses)
                
                with col3:
                    goals_for = team_matches['team_goals'].sum()
                    goals_against = team_matches['opponent_goals'].sum()
                    goal_diff = goals_for - goals_against
                    
                    st.metric("Goals For", goals_for)
                    st.metric("Goals Against", goals_against)
                    st.metric("Goal Difference", goal_diff)
                
                # Form chart
                st.subheader("Recent Form")
                
                recent_matches = team_matches.sort_values('date', ascending=False).head(10)
                recent_form = recent_matches[['date', 'opponent', 'team_goals', 'opponent_goals', 'result']]
                
                # Color mapping for results
                color_map = {'Win': '#28a745', 'Draw': '#ffc107', 'Loss': '#dc3545'}
                recent_form['color'] = recent_form['result'].map(color_map)
                
                fig_form = go.Figure()
                
                for i, match in recent_form.iterrows():
                    fig_form.add_trace(go.Scatter(
                        x=[match['date']],
                        y=[1],
                        mode='markers',
                        marker=dict(
                            size=30,
                            color=match['color']
                        ),
                        text=f"{match['opponent']} ({match['team_goals']}-{match['opponent_goals']})",
                        hoverinfo='text',
                        showlegend=False
                    ))
                
                fig_form.update_layout(
                    title="Recent Form (Last 10 Matches)",
                    xaxis=dict(title="Date"),
                    yaxis=dict(visible=False),
                    height=200
                )
                
                st.plotly_chart(fig_form, use_container_width=True)
                
                # Result distribution
                st.subheader("Result Distribution")
                
                result_counts = team_matches['result'].value_counts().reset_index()
                result_counts.columns = ['Result', 'Count']
                
                fig_results = px.pie(
                    result_counts,
                    values='Count',
                    names='Result',
                    color='Result',
                    title='Match Results',
                    color_discrete_map={'Win': '#28a745', 'Draw': '#ffc107', 'Loss': '#dc3545'}
                )
                
                st.plotly_chart(fig_results, use_container_width=True)
                
                # xG analysis
                st.subheader("Expected Goals (xG) Analysis")
                
                xg_df = team_matches[['date', 'opponent', 'team_goals', 'opponent_goals', 'team_xg', 'opponent_xg']]
                xg_df['xg_diff'] = xg_df['team_xg'] - xg_df['opponent_xg']
                xg_df['goal_diff'] = xg_df['team_goals'] - xg_df['opponent_goals']
                
                fig_xg = go.Figure()
                
                fig_xg.add_trace(go.Bar(
                    x=xg_df['date'],
                    y=xg_df['team_xg'],
                    name='Team xG',
                    marker_color='rgba(0, 123, 255, 0.7)'
                ))
                
                fig_xg.add_trace(go.Bar(
                    x=xg_df['date'],
                    y=xg_df['opponent_xg'],
                    name='Opponent xG',
                    marker_color='rgba(220, 53, 69, 0.7)'
                ))
                
                fig_xg.add_trace(go.Scatter(
                    x=xg_df['date'],
                    y=xg_df['team_goals'],
                    name='Team Goals',
                    mode='markers',
                    marker=dict(size=10, color='rgba(0, 123, 255, 1)')
                ))
                
                fig_xg.add_trace(go.Scatter(
                    x=xg_df['date'],
                    y=xg_df['opponent_goals'],
                    name='Opponent Goals',
                    mode='markers',
                    marker=dict(size=10, color='rgba(220, 53, 69, 1)')
                ))
                
                fig_xg.update_layout(
                    title=f"{selected_team} - Expected vs Actual Goals",
                    xaxis_title="Match Date",
                    yaxis_title="Goals / xG",
                    barmode='group'
                )
                
                st.plotly_chart(fig_xg, use_container_width=True)
                
                # xG difference vs goal difference
                st.subheader("xG Difference vs Goal Difference")
                
                fig_diffs = go.Figure()
                
                fig_diffs.add_trace(go.Bar(
                    x=xg_df['date'],
                    y=xg_df['xg_diff'],
                    name='xG Difference',
                    marker_color='rgba(255, 193, 7, 0.7)'
                ))
                
                fig_diffs.add_trace(go.Bar(
                    x=xg_df['date'],
                    y=xg_df['goal_diff'],
                    name='Goal Difference',
                    marker_color='rgba(40, 167, 69, 0.7)'
                ))
                
                fig_diffs.update_layout(
                    title=f"{selected_team} - xG Difference vs Goal Difference",
                    xaxis_title="Match Date",
                    yaxis_title="Difference",
                    barmode='group'
                )
                
                st.plotly_chart(fig_diffs, use_container_width=True)
                
                # Get player stats for the team
                player_stats = load_player_stats(league_filter, season_filter)
                team_players = player_stats[player_stats['team'] == selected_team].sort_values('total_goals', ascending=False)
                
                if not team_players.empty:
                    st.subheader(f"{selected_team} - Player Statistics")
                    
                    st.dataframe(
                        team_players[['player_name', 'position', 'total_goals', 'total_assists', 
                                     'total_xg', 'total_xa', 'matches_played']],
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Top contributors chart
                    top_contributors = team_players.head(10)
                    
                    fig_contributors = go.Figure()
                    
                    fig_contributors.add_trace(go.Bar(
                        x=top_contributors['player_name'],
                        y=top_contributors['total_goals'],
                        name='Goals',
                        marker_color='#3366CC'
                    ))
                    
                    fig_contributors.add_trace(go.Bar(
                        x=top_contributors['player_name'],
                        y=top_contributors['total_assists'],
                        name='Assists',
                        marker_color='#FF6633'
                    ))
                    
                    fig_contributors.update_layout(
                        title=f"{selected_team} - Top Goal Contributors",
                        xaxis_title="Player",
                        yaxis_title="Count",
                        barmode='group'
                    )
                    
                    st.plotly_chart(fig_contributors, use_container_width=True)

elif page == "Match Results":
    st.title("Match Results")
    
    if not leagues:
        st.warning("No data available. Please ensure the data pipeline is running and data is being collected.")
    else:
        # Load recent matches
        matches = load_matches(league_filter, season_filter)
        
        if matches.empty:
            st.warning("No match data found for the selected filters.")
        else:
            # Process matches data
            matches['date'] = pd.to_datetime(matches['date'])
            matches = matches.sort_values('date', ascending=False)
            
            # Add result column
            matches['result'] = matches.apply(
                lambda x: 'Home Win' if x['home_goals'] > x['away_goals'] else 
                         ('Draw' if x['home_goals'] == x['away_goals'] else 'Away Win'), 
                axis=1
            )
            
            # Display recent matches
            st.subheader("Recent Matches")
            
            # Format matches for display
            display_matches = matches.copy()
            display_matches['match'] = display_matches.apply(
                lambda x: f"{x['home_team']} {x['home_goals']} - {x['away_goals']} {x['away_team']}", 
                axis=1
            )
            display_matches['xg'] = display_matches.apply(
                lambda x: f"{x['home_xg']:.2f} - {x['away_xg']:.2f}", 
                axis=1
            )
            
            st.dataframe(
                display_matches[['date', 'match', 'xg', 'league', 'season', 'result']].rename(
                    columns={
                        'date': 'Date',
                        'match': 'Match',
                        'xg': 'xG',
                        'league': 'League',
                        'season': 'Season',
                        'result': 'Result'
                    }
                ),
                use_container_width=True,
                hide_index=True
            )
            
            # Result distribution
            st.subheader("Result Distribution")
            
            result_counts = matches['result'].value_counts().reset_index()
            result_counts.columns = ['Result', 'Count']
            
            fig_results = px.pie(
                result_counts,
                values='Count',
                names='Result',
                title='Match Results',
                color='Result',
                color_discrete_map={
                    'Home Win': '#28a745', 
                    'Draw': '#ffc107', 
                    'Away Win': '#dc3545'
                }
            )
            
            st.plotly_chart(fig_results, use_container_width=True)
            
            # Home vs Away goals
            st.subheader("Home vs Away Goals")
            
            matches_by_date = matches.sort_values('date')
            
            fig_goals = go.Figure()
            
            fig_goals.add_trace(go.Scatter(
                x=matches_by_date['date'],
                y=matches_by_date['home_goals'].rolling(window=10).mean(),
                name='Home Goals (10-match avg)',
                line=dict(color='#3366CC', width=2)
            ))
            
            fig_goals.add_trace(go.Scatter(
                x=matches_by_date['date'],
                y=matches_by_date['away_goals'].rolling(window=10).mean(),
                name='Away Goals (10-match avg)',
                line=dict(color='#DC3545', width=2)
            ))
            
            fig_goals.update_layout(
                title="Average Goals per Match (10-match Rolling Average)",
                xaxis_title="Date",
                yaxis_title="Goals"
            )
            
            st.plotly_chart(fig_goals, use_container_width=True)
            
            # xG analysis
            st.subheader("Expected Goals (xG) Analysis")
            
            # Calculate xG accuracy (how well xG predicts match outcome)
            matches['xg_predicted_winner'] = matches.apply(
                lambda x: 'home' if x['home_xg'] > x['away_xg'] else 
                         ('draw' if x['home_xg'] == x['away_xg'] else 'away'), 
                axis=1
            )
            
            matches['actual_winner'] = matches.apply(
                lambda x: 'home' if x['home_goals'] > x['away_goals'] else 
                         ('draw' if x['home_goals'] == x['away_goals'] else 'away'), 
                axis=1
            )
            
            matches['xg_prediction_correct'] = matches['xg_predicted_winner'] == matches['actual_winner']
            
            xg_accuracy = matches['xg_prediction_correct'].mean() * 100
            
            st.metric("xG Prediction Accuracy", f"{xg_accuracy:.1f}%")
            
            # xG vs actual goals scatter plot
            fig_xg_scatter = go.Figure()
            
            fig_xg_scatter.add_trace(go.Scatter(
                x=matches['home_xg'],
                y=matches['home_goals'],
                mode='markers',
                name='Home Team',
                marker=dict(
                    size=8,
                    color='#3366CC',
                    opacity=0.7
                )
            ))
            
            fig_xg_scatter.add_trace(go.Scatter(
                x=matches['away_xg'],
                y=matches['away_goals'],
                mode='markers',
                name='Away Team',
                marker=dict(
                    size=8,
                    color='#DC3545',
                    opacity=0.7
                )
            ))
            

elif page == "Shot Analysis":
    st.title("Shot Analysis")
    
    if not leagues:
        st.warning("No data available. Please ensure the data pipeline is running and data is being collected.")
    else:
        # Get available teams based on filters
        teams = get_teams(league_filter, season_filter)
        
        if not teams:
            st.warning("No teams found for the selected filters.")
        else:
            # Team selection
            selected_team = st.selectbox("Select Team", teams)
            
            # Load shot data
            shot_data = load_shot_heatmap_data(selected_team, league_filter, season_filter)
            
            if shot_data.empty:
                st.warning(f"No shot data found for {selected_team}.")
            else:
                # Shot conversion stats
                st.subheader("Shot Statistics")
                
                total_shots = len(shot_data)
                goals = len(shot_data[shot_data['result'] == 'Goal'])
                shot_conversion = (goals / total_shots) * 100 if total_shots > 0 else 0
                total_xg = shot_data['xg'].sum()
                avg_xg_per_shot = total_xg / total_shots if total_shots > 0 else 0
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Total Shots", total_shots)
                    st.metric("Goals", goals)
                
                with col2:
                    st.metric("Shot Conversion", f"{shot_conversion:.1f}%")
                    st.metric("xG per Shot", f"{avg_xg_per_shot:.3f}")
                
                with col3:
                    st.metric("Total xG", f"{total_xg:.2f}")
                    st.metric("Goals - xG", f"{goals - total_xg:.2f}")
                
                # Shot heatmap
                st.subheader("Shot Locations Heatmap")
                
                pitch_width = 100
                pitch_height = 100
                
                fig_heatmap = go.Figure()
                
                # Add pitch outline
                fig_heatmap.add_shape(type="rect",
                    x0=0, y0=0, x1=pitch_width, y1=pitch_height,
                    line=dict(color="rgba(0,0,0,0.5)", width=2),
                    fillcolor="rgba(0,255,0,0.1)"
                )
                
                # Add penalty areas
                fig_heatmap.add_shape(type="rect",
                    x0=0, y0=18, x1=18, y1=pitch_height-18,
                    line=dict(color="rgba(0,0,0,0.5)", width=1),
                    fillcolor="rgba(0,0,0,0)"
                )
                
                fig_heatmap.add_shape(type="rect",
                    x0=pitch_width-18, y0=18, x1=pitch_width, y1=pitch_height-18,
                    line=dict(color="rgba(0,0,0,0.5)", width=1),
                    fillcolor="rgba(0,0,0,0)"
                )
                
                # Add goals
                fig_heatmap.add_shape(type="rect",
                    x0=0, y0=36, x1=2, y1=pitch_height-36,
                    line=dict(color="rgba(0,0,0,0.5)", width=1),
                    fillcolor="rgba(0,0,0,0)"
                )
                
                fig_heatmap.add_shape(type="rect",
                    x0=pitch_width-2, y0=36, x1=pitch_width, y1=pitch_height-36,
                    line=dict(color="rgba(0,0,0,0.5)", width=1),
                    fillcolor="rgba(0,0,0,0)"
                )
                
                # Convert coordinates to match the pitch dimensions
                # Note: Understat coordinates might need adjustment based on their format
                shot_x = shot_data['x'] * pitch_width / 100
                shot_y = shot_data['y'] * pitch_height / 100
                
                # Add shots as scatter points
                colors = shot_data['result'].map({'Goal': 'red', 'MissedShots': 'yellow', 'SavedShot': 'blue', 
                                               'BlockedShot': 'green', 'ShotOnPost': 'purple'})
                sizes = shot_data['xg'] * 50 + 5  # Scale xG for better visualization
                
                fig_heatmap.add_trace(go.Scatter(
                    x=shot_x,
                    y=shot_y,
                    mode='markers',
                    marker=dict(
                        size=sizes,
                        color=colors,
                        opacity=0.7
                    ),
                    text=[f"xG: {xg:.3f}, Result: {result}" for xg, result in zip(shot_data['xg'], shot_data['result'])],
                    hoverinfo='text',
                    name='Shots'
                ))
                
                fig_heatmap.update_layout(
                    title=f"{selected_team} - Shot Map",
                    xaxis=dict(title="", showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(title="", showgrid=False, zeroline=False, showticklabels=False),
                    autosize=False,
                    width=700,
                    height=500,
                    margin=dict(l=50, r=50, b=50, t=50),
                )
                
                st.plotly_chart(fig_heatmap, use_container_width=True)
                
                # Shot situations breakdown
                st.subheader("Shot Situations")
                
                situation_counts = shot_data['situation'].value_counts().reset_index()
                situation_counts.columns = ['Situation', 'Count']
                
                fig_situations = px.pie(
                    situation_counts,
                    values='Count',
                    names='Situation',
                    title='Shot Situations',
                    color_discrete_sequence=px.colors.qualitative.Plotly
                )
                
                st.plotly_chart(fig_situations, use_container_width=True)
                
                # xG distribution
                st.subheader("Expected Goals (xG) Distribution")
                
                fig_xg_dist = px.histogram(
                    shot_data,
                    x='xg',
                    nbins=20,
                    color='result',
                    marginal='box',
                    title='xG Distribution by Result',
                    labels={'xg': 'Expected Goals (xG)', 'count': 'Number of Shots'}
                )
                
                st.plotly_chart(fig_xg_dist, use_container_width=True)

# Run the Streamlit app with: streamlit run app.py