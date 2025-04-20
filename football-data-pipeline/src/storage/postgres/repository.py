import uuid
import yaml
from typing import Dict, Any, List, Optional, Tuple, Type
from datetime import datetime
from sqlalchemy import create_engine, desc, asc, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

from config.settings import config
from src.utils.logging_utils import get_logger
from src.storage.postgres.models import (
    Base, League, Season, Team, Player, Match, 
    MatchStats, PlayerMatchStats, TeamSeasonStats, MatchEvent
)


class PostgresRepository:
    """Repository for managing football data in PostgreSQL"""
    
    def __init__(self, connection_string: str = None):
        """
        Initialize PostgreSQL repository
        
        Args:
            connection_string: Database connection string (optional)
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # Use provided connection string or load from config
        if connection_string is None:
            postgres_config = config.get_storage_config()
            connection_string = (
                f"postgresql://{postgres_config['user']}:{postgres_config['password']}@"
                f"{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
            )
        
        self.engine = create_engine(connection_string)
        self.SessionFactory = sessionmaker(bind=self.engine)
    
    @contextmanager
    def session_scope(self):
        """
        Context manager for database sessions
        
        Yields:
            SQLAlchemy session
        """
        session = self.SessionFactory()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Database error: {str(e)}")
            raise
        finally:
            session.close()
    
    def create_tables(self):
        """Create all database tables"""
        try:
            Base.metadata.create_all(self.engine)
            self.logger.info("Created all database tables")
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to create tables: {str(e)}")
            raise
    
    def drop_tables(self):
        """Drop all database tables"""
        try:
            Base.metadata.drop_all(self.engine)
            self.logger.info("Dropped all database tables")
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to drop tables: {str(e)}")
            raise
    
    def _generate_id(self) -> str:
        """Generate a unique ID for database entities"""
        return str(uuid.uuid4())
    
    # League methods
    
    def save_league(self, league_data: Dict[str, Any]) -> League:
        """
        Save league data to the database
        
        Args:
            league_data: League data dictionary
            
        Returns:
            Saved League object
        """
        with self.session_scope() as session:
            league_id = league_data.get("id")
            
            # Check if league already exists
            existing_league = session.query(League).filter(League.id == league_id).first()
            
            if existing_league:
                # Update existing league
                for key, value in league_data.items():
                    if hasattr(existing_league, key):
                        setattr(existing_league, key, value)
                
                league = existing_league
                self.logger.info(f"Updated league: {league.name}")
            else:
                # Create new league
                league = League(**league_data)
                session.add(league)
                self.logger.info(f"Saved new league: {league.name}")
            
            session.flush()
            return league
    
    def get_league(self, league_id: str) -> Optional[Dict[str, Any]]:
        """
        Get league data by ID
        
        Args:
            league_id: League ID
            
        Returns:
            League data dictionary or None if not found
        """
        with self.session_scope() as session:
            league = session.query(League).filter(League.id == league_id).first()
            
            if league:
                return {
                    "id": league.id,
                    "name": league.name,
                    "country": league.country
                }
            
            return None
    
    def get_all_leagues(self) -> List[Dict[str, Any]]:
        """
        Get all leagues
        
        Returns:
            List of league data dictionaries
        """
        with self.session_scope() as session:
            leagues = session.query(League).all()
            
            return [
                {
                    "id": league.id,
                    "name": league.name,
                    "country": league.country
                }
                for league in leagues
            ]
    
    # Season methods
    
    def save_season(self, season_data: Dict[str, Any]) -> Season:
        """
        Save season data to the database
        
        Args:
            season_data: Season data dictionary
            
        Returns:
            Saved Season object
        """
        with self.session_scope() as session:
            season_id = season_data.get("id")
            
            # Check if season already exists
            existing_season = session.query(Season).filter(Season.id == season_id).first()
            
            if existing_season:
                # Update existing season
                for key, value in season_data.items():
                    if hasattr(existing_season, key):
                        setattr(existing_season, key, value)
                
                season = existing_season
                self.logger.info(f"Updated season: {season.year} for league {season.league_id}")
            else:
                # Create new season
                season = Season(**season_data)
                session.add(season)
                self.logger.info(f"Saved new season: {season.year} for league {season.league_id}")
            
            session.flush()
            return season
    
    def get_season(self, season_id: str) -> Optional[Dict[str, Any]]:
        """
        Get season data by ID
        
        Args:
            season_id: Season ID
            
        Returns:
            Season data dictionary or None if not found
        """
        with self.session_scope() as session:
            season = session.query(Season).filter(Season.id == season_id).first()
            
            if season:
                return {
                    "id": season.id,
                    "year": season.year,
                    "league_id": season.league_id,
                    "is_current": season.is_current
                }
            
            return None
    
    def get_seasons_for_league(self, league_id: str) -> List[Dict[str, Any]]:
        """
        Get all seasons for a league
        
        Args:
            league_id: League ID
            
        Returns:
            List of season data dictionaries
        """
        with self.session_scope() as session:
            seasons = session.query(Season).filter(Season.league_id == league_id).all()
            
            return [
                {
                    "id": season.id,
                    "year": season.year,
                    "league_id": season.league_id,
                    "is_current": season.is_current
                }
                for season in seasons
            ]
    
    # Team methods
    
    def save_team(self, team_data: Dict[str, Any]) -> Team:
        """
        Save team data to the database
        
        Args:
            team_data: Team data dictionary
            
        Returns:
            Saved Team object
        """
        with self.session_scope() as session:
            team_id = team_data.get("id")
            
            # Check if team already exists
            existing_team = session.query(Team).filter(Team.id == team_id).first()
            
            if existing_team:
                # Update existing team
                for key, value in team_data.items():
                    if hasattr(existing_team, key):
                        setattr(existing_team, key, value)
                
                team = existing_team
                self.logger.info(f"Updated team: {team.name}")
            else:
                # Create new team
                team = Team(**team_data)
                session.add(team)
                self.logger.info(f"Saved new team: {team.name}")
            
            session.flush()
            return team
    
    def get_team(self, team_id: str) -> Optional[Dict[str, Any]]:
        """
        Get team data by ID
        
        Args:
            team_id: Team ID
            
        Returns:
            Team data dictionary or None if not found
        """
        with self.session_scope() as session:
            team = session.query(Team).filter(Team.id == team_id).first()
            
            if team:
                return {
                    "id": team.id,
                    "name": team.name,
                    "short_name": team.short_name
                }
            
            return None
    
    def get_teams_by_name(self, name: str) -> List[Dict[str, Any]]:
        """
        Get teams by name (partial match)
        
        Args:
            name: Team name (partial match)
            
        Returns:
            List of team data dictionaries
        """
        with self.session_scope() as session:
            teams = session.query(Team).filter(Team.name.ilike(f"%{name}%")).all()
            
            return [
                {
                    "id": team.id,
                    "name": team.name,
                    "short_name": team.short_name
                }
                for team in teams
            ]
    
    def get_teams_for_season(self, season_id: str) -> List[Dict[str, Any]]:
        """
        Get all teams participating in a season
        
        Args:
            season_id: Season ID
            
        Returns:
            List of team data dictionaries
        """
        with self.session_scope() as session:
            # Get teams from matches where they're either home or away team
            teams_query = (
                session.query(Team)
                .join(Match, (Team.id == Match.home_team_id) | (Team.id == Match.away_team_id))
                .filter(Match.season_id == season_id)
                .distinct()
            )
            
            teams = teams_query.all()
            
            return [
                {
                    "id": team.id,
                    "name": team.name,
                    "short_name": team.short_name
                }
                for team in teams
            ]
    
    # Player methods
    
    def save_player(self, player_data: Dict[str, Any]) -> Player:
        """
        Save player data to the database
        
        Args:
            player_data: Player data dictionary
            
        Returns:
            Saved Player object
        """
        with self.session_scope() as session:
            player_id = player_data.get("id")
            
            # Check if player already exists
            existing_player = session.query(Player).filter(Player.id == player_id).first()
            
            if existing_player:
                # Update existing player
                for key, value in player_data.items():
                    if hasattr(existing_player, key):
                        setattr(existing_player, key, value)
                
                player = existing_player
                self.logger.info(f"Updated player: {player.name}")
            else:
                # Create new player
                player = Player(**player_data)
                session.add(player)
                self.logger.info(f"Saved new player: {player.name}")
            
            session.flush()
            return player
    
    def get_player(self, player_id: str) -> Optional[Dict[str, Any]]:
        """
        Get player data by ID
        
        Args:
            player_id: Player ID
            
        Returns:
            Player data dictionary or None if not found
        """
        with self.session_scope() as session:
            player = session.query(Player).filter(Player.id == player_id).first()
            
            if player:
                return {
                    "id": player.id,
                    "name": player.name,
                    "position": player.position,
                    "nationality": player.nationality
                }
            
            return None
    
    def get_players_by_name(self, name: str) -> List[Dict[str, Any]]:
        """
        Get players by name (partial match)
        
        Args:
            name: Player name (partial match)
            
        Returns:
            List of player data dictionaries
        """
        with self.session_scope() as session:
            players = session.query(Player).filter(Player.name.ilike(f"%{name}%")).all()
            
            return [
                {
                    "id": player.id,
                    "name": player.name,
                    "position": player.position,
                    "nationality": player.nationality
                }
                for player in players
            ]
    
    def get_players_for_team(self, team_id: str, season_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all players for a team, optionally filtered by season
        
        Args:
            team_id: Team ID
            season_id: Optional Season ID to filter by
            
        Returns:
            List of player data dictionaries
        """
        with self.session_scope() as session:
            query = (
                session.query(Player)
                .join(PlayerMatchStats, Player.id == PlayerMatchStats.player_id)
                .join(Match, PlayerMatchStats.match_id == Match.id)
                .filter((Match.home_team_id == team_id) | (Match.away_team_id == team_id))
            )
            
            if season_id:
                query = query.filter(Match.season_id == season_id)
            
            players = query.distinct().all()
            
            return [
                {
                    "id": player.id,
                    "name": player.name,
                    "position": player.position,
                    "nationality": player.nationality
                }
                for player in players
            ]
    
    # Match methods
    
    def save_match(self, match_data: Dict[str, Any]) -> Match:
        """
        Save match data to the database
        
        Args:
            match_data: Match data dictionary
            
        Returns:
            Saved Match object
        """
        with self.session_scope() as session:
            match_id = match_data.get("id")
            
            # Check if match already exists
            existing_match = session.query(Match).filter(Match.id == match_id).first()
            
            if existing_match:
                # Update existing match
                for key, value in match_data.items():
                    if hasattr(existing_match, key):
                        setattr(existing_match, key, value)
                
                match = existing_match
                self.logger.info(f"Updated match: {match.id}")
            else:
                # Create new match
                match = Match(**match_data)
                session.add(match)
                self.logger.info(f"Saved new match: {match.id}")
            
            session.flush()
            return match
    
    def get_match(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Get match data by ID
        
        Args:
            match_id: Match ID
            
        Returns:
            Match data dictionary or None if not found
        """
        with self.session_scope() as session:
            match = session.query(Match).filter(Match.id == match_id).first()
            
            if match:
                return {
                    "id": match.id,
                    "season_id": match.season_id,
                    "home_team_id": match.home_team_id,
                    "away_team_id": match.away_team_id,
                    "date": match.date.isoformat() if match.date else None,
                    "gameweek": match.gameweek
                }
            
            return None
    
    def get_matches_for_team(
        self, team_id: str, season_id: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get matches for a team, optionally filtered by season
        
        Args:
            team_id: Team ID
            season_id: Optional Season ID to filter by
            limit: Maximum number of matches to return
            offset: Number of matches to skip
            
        Returns:
            List of match data dictionaries
        """
        with self.session_scope() as session:
            query = (
                session.query(Match)
                .filter((Match.home_team_id == team_id) | (Match.away_team_id == team_id))
            )
            
            if season_id:
                query = query.filter(Match.season_id == season_id)
            
            matches = query.order_by(desc(Match.date)).limit(limit).offset(offset).all()
            
            return [
                {
                    "id": match.id,
                    "season_id": match.season_id,
                    "home_team_id": match.home_team_id,
                    "away_team_id": match.away_team_id,
                    "date": match.date.isoformat() if match.date else None,
                    "gameweek": match.gameweek
                }
                for match in matches
            ]
    
    def get_matches_for_season(
        self, season_id: str, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get matches for a season
        
        Args:
            season_id: Season ID
            limit: Maximum number of matches to return
            offset: Number of matches to skip
            
        Returns:
            List of match data dictionaries
        """
        with self.session_scope() as session:
            matches = (
                session.query(Match)
                .filter(Match.season_id == season_id)
                .order_by(desc(Match.date))
                .limit(limit)
                .offset(offset)
                .all()
            )
            
            return [
                {
                    "id": match.id,
                    "season_id": match.season_id,
                    "home_team_id": match.home_team_id,
                    "away_team_id": match.away_team_id,
                    "date": match.date.isoformat() if match.date else None,
                    "gameweek": match.gameweek
                }
                for match in matches
            ]
    
    # Match stats methods
    
    def save_match_stats(self, match_stats_data: Dict[str, Any]) -> MatchStats:
        """
        Save match statistics to the database
        
        Args:
            match_stats_data: Match statistics data dictionary
            
        Returns:
            Saved MatchStats object
        """
        with self.session_scope() as session:
            match_id = match_stats_data.get("match_id")
            
            # Check if match stats already exist
            existing_match_stats = (
                session.query(MatchStats).filter(MatchStats.match_id == match_id).first()
            )
            
            if existing_match_stats:
                # Update existing match stats
                for key, value in match_stats_data.items():
                    if hasattr(existing_match_stats, key):
                        setattr(existing_match_stats, key, value)
                
                match_stats = existing_match_stats
                self.logger.info(f"Updated match stats for match: {match_id}")
            else:
                # Create new match stats
                match_stats = MatchStats(**match_stats_data)
                session.add(match_stats)
                self.logger.info(f"Saved new match stats for match: {match_id}")
            
            session.flush()
            return match_stats
    
    def get_match_stats(self, match_id: str) -> Optional[Dict[str, Any]]:
        """
        Get match statistics by match ID
        
        Args:
            match_id: Match ID
            
        Returns:
            Match statistics dictionary or None if not found
        """
        with self.session_scope() as session:
            match_stats = (
                session.query(MatchStats).filter(MatchStats.match_id == match_id).first()
            )
            
            if match_stats:
                return {
                    "id": match_stats.id,
                    "match_id": match_stats.match_id,
                    "home_goals": match_stats.home_goals,
                    "away_goals": match_stats.away_goals,
                    "home_xg": match_stats.home_xg,
                    "away_xg": match_stats.away_xg,
                    "home_shots": match_stats.home_shots,
                    "away_shots": match_stats.away_shots,
                    "home_shots_on_target": match_stats.home_shots_on_target,
                    "away_shots_on_target": match_stats.away_shots_on_target,
                    "home_possession": match_stats.home_possession,
                    "away_possession": match_stats.away_possession
                }
            
            return None
    
    # Player match stats methods
    
    def save_player_match_stats(
        self, player_match_stats_data: Dict[str, Any]
    ) -> PlayerMatchStats:
        """
        Save player match statistics to the database
        
        Args:
            player_match_stats_data: Player match statistics data dictionary
            
        Returns:
            Saved PlayerMatchStats object
        """
        with self.session_scope() as session:
            player_id = player_match_stats_data.get("player_id")
            match_id = player_match_stats_data.get("match_id")
            
            # Check if player match stats already exist
            existing_stats = (
                session.query(PlayerMatchStats)
                .filter(
                    PlayerMatchStats.player_id == player_id,
                    PlayerMatchStats.match_id == match_id
                )
                .first()
            )
            
            if existing_stats:
                # Update existing player match stats
                for key, value in player_match_stats_data.items():
                    if hasattr(existing_stats, key):
                        setattr(existing_stats, key, value)
                
                player_match_stats = existing_stats
                self.logger.info(f"Updated stats for player: {player_id} in match: {match_id}")
            else:
                # Create new player match stats
                player_match_stats = PlayerMatchStats(**player_match_stats_data)
                session.add(player_match_stats)
                self.logger.info(f"Saved new stats for player: {player_id} in match: {match_id}")
            
            session.flush()
            return player_match_stats
    
    def get_player_match_stats(
        self, player_id: str, match_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get player statistics for a match
        
        Args:
            player_id: Player ID
            match_id: Match ID
            
        Returns:
            Player match statistics dictionary or None if not found
        """
        with self.session_scope() as session:
            player_stats = (
                session.query(PlayerMatchStats)
                .filter(
                    PlayerMatchStats.player_id == player_id,
                    PlayerMatchStats.match_id == match_id
                )
                .first()
            )
            
            if player_stats:
                return {
                    "id": player_stats.id,
                    "player_id": player_stats.player_id,
                    "match_id": player_stats.match_id,
                    "team_id": player_stats.team_id,
                    "minutes_played": player_stats.minutes_played,
                    "goals": player_stats.goals,
                    "assists": player_stats.assists,
                    "shots": player_stats.shots,
                    "xg": player_stats.xg,
                    "key_passes": player_stats.key_passes,
                    "xa": player_stats.xa,
                    "position": player_stats.position
                }
            
            return None
    
    def get_player_stats_for_match(self, match_id: str) -> List[Dict[str, Any]]:
        """
        Get all player statistics for a match
        
        Args:
            match_id: Match ID
            
        Returns:
            List of player match statistics dictionaries
        """
        with self.session_scope() as session:
            player_stats = (
                session.query(PlayerMatchStats)
                .filter(PlayerMatchStats.match_id == match_id)
                .all()
            )
            
            return [
                {
                    "id": stats.id,
                    "player_id": stats.player_id,
                    "match_id": stats.match_id,
                    "team_id": stats.team_id,
                    "minutes_played": stats.minutes_played,
                    "goals": stats.goals,
                    "assists": stats.assists,
                    "shots": stats.shots,
                    "xg": stats.xg,
                    "key_passes": stats.key_passes,
                    "xa": stats.xa,
                    "position": stats.position
                }
                for stats in player_stats
            ]
    
    # Team season stats methods
    
    def save_team_season_stats(
        self, team_season_stats_data: Dict[str, Any]
    ) -> TeamSeasonStats:
        """
        Save team season statistics to the database
        
        Args:
            team_season_stats_data: Team season statistics data dictionary
            
        Returns:
            Saved TeamSeasonStats object
        """
        with self.session_scope() as session:
            team_id = team_season_stats_data.get("team_id")
            season_id = team_season_stats_data.get("season_id")
            
            # Check if team season stats already exist
            existing_stats = (
                session.query(TeamSeasonStats)
                .filter(
                    TeamSeasonStats.team_id == team_id,
                    TeamSeasonStats.season_id == season_id
                )
                .first()
            )
            
            if existing_stats:
                # Update existing team season stats
                for key, value in team_season_stats_data.items():
                    if hasattr(existing_stats, key):
                        setattr(existing_stats, key, value)
                
                team_season_stats = existing_stats
                self.logger.info(f"Updated stats for team: {team_id} in season: {season_id}")
            else:
                # Create new team season stats
                team_season_stats = TeamSeasonStats(**team_season_stats_data)
                session.add(team_season_stats)
                self.logger.info(f"Saved new stats for team: {team_id} in season: {season_id}")
            
            session.flush()
            return team_season_stats
    
    def get_team_season_stats(
        self, team_id: str, season_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get team statistics for a season
        
        Args:
            team_id: Team ID
            season_id: Season ID
            
        Returns:
            Team season statistics dictionary or None if not found
        """
        with self.session_scope() as session:
            team_stats = (
                session.query(TeamSeasonStats)
                .filter(
                    TeamSeasonStats.team_id == team_id,
                    TeamSeasonStats.season_id == season_id
                )
                .first()
            )
            
            if team_stats:
                return {
                    "id": team_stats.id,
                    "team_id": team_stats.team_id,
                    "season_id": team_stats.season_id,
                    "matches_played": team_stats.matches_played,
                    "wins": team_stats.wins,
                    "draws": team_stats.draws,
                    "losses": team_stats.losses,
                    "goals_for": team_stats.goals_for,
                    "goals_against": team_stats.goals_against,
                    "points": team_stats.points,
                    "position": team_stats.position,
                    "xg_for": team_stats.xg_for,
                    "xg_against": team_stats.xg_against
                }
            
            return None
    
    def get_season_standings(self, season_id: str) -> List[Dict[str, Any]]:
        """
        Get team standings for a season
        
        Args:
            season_id: Season ID
            
        Returns:
            List of team season statistics dictionaries, ordered by position
        """
        with self.session_scope() as session:
            team_stats = (
                session.query(TeamSeasonStats)
                .filter(TeamSeasonStats.season_id == season_id)
                .order_by(asc(TeamSeasonStats.position))
                .all()
            )
            
            return [
                {
                    "id": stats.id,
                    "team_id": stats.team_id,
                    "season_id": stats.season_id,
                    "matches_played": stats.matches_played,
                    "wins": stats.wins,
                    "draws": stats.draws,
                    "losses": stats.losses,
                    "goals_for": stats.goals_for,
                    "goals_against": stats.goals_against,
                    "points": stats.points,
                    "position": stats.position,
                    "xg_for": stats.xg_for,
                    "xg_against": stats.xg_against
                }
                for stats in team_stats
            ]
    
    # Match events methods
    
    def save_match_event(self, match_event_data: Dict[str, Any]) -> MatchEvent:
        """
        Save match event to the database
        
        Args:
            match_event_data: Match event data dictionary
            
        Returns:
            Saved MatchEvent object
        """
        with self.session_scope() as session:
            match_event = MatchEvent(**match_event_data)
            session.add(match_event)
            session.flush()
            
            self.logger.info(f"Saved match event for match: {match_event.match_id}")
            return match_event
    
    def get_match_events(self, match_id: str) -> List[Dict[str, Any]]:
        """
        Get all events for a match
        
        Args:
            match_id: Match ID
            
        Returns:
            List of match event dictionaries
        """
        with self.session_scope() as session:
            events = (
                session.query(MatchEvent)
                .filter(MatchEvent.match_id == match_id)
                .order_by(asc(MatchEvent.minute))
                .all()
            )
            
            return [
                {
                    "id": event.id,
                    "match_id": event.match_id,
                    "team_id": event.team_id,
                    "player_id": event.player_id,
                    "event_type": event.event_type,
                    "minute": event.minute,
                    "xg": event.xg,
                    "id": event.id,
                    "match_id": event.match_id,
                    "team_id": event.team_id,
                    "player_id": event.player_id,
                    "event_type": event.event_type,
                    "minute": event.minute,
                    "xg": event.xg,
                    "xa": event.xa,
                    "second_player_id": event.second_player_id,
                    "shot_type": event.shot_type,
                    "position_x": event.position_x,
                    "position_y": event.position_y,
                    "is_goal": event.is_goal
                }
                for event in events
            ]
    
    # Advanced querying methods
    
    def get_player_season_stats(self, player_id: str, season_id: str) -> Dict[str, Any]:
        """
        Get aggregated player statistics for a season
        
        Args:
            player_id: Player ID
            season_id: Season ID
            
        Returns:
            Aggregated player statistics dictionary
        """
        with self.session_scope() as session:
            result = (
                session.query(
                    func.count(PlayerMatchStats.match_id).label("matches_played"),
                    func.sum(PlayerMatchStats.minutes_played).label("total_minutes"),
                    func.sum(PlayerMatchStats.goals).label("goals"),
                    func.sum(PlayerMatchStats.assists).label("assists"),
                    func.sum(PlayerMatchStats.shots).label("shots"),
                    func.sum(PlayerMatchStats.xg).label("xg"),
                    func.sum(PlayerMatchStats.key_passes).label("key_passes"),
                    func.sum(PlayerMatchStats.xa).label("xa")
                )
                .join(Match, PlayerMatchStats.match_id == Match.id)
                .filter(
                    PlayerMatchStats.player_id == player_id,
                    Match.season_id == season_id
                )
                .first()
            )
            
            if result:
                return {
                    "player_id": player_id,
                    "season_id": season_id,
                    "matches_played": result.matches_played or 0,
                    "total_minutes": result.total_minutes or 0,
                    "goals": result.goals or 0,
                    "assists": result.assists or 0,
                    "shots": result.shots or 0,
                    "xg": float(result.xg) if result.xg else 0.0,
                    "key_passes": result.key_passes or 0,
                    "xa": float(result.xa) if result.xa else 0.0,
                    "goals_per_90": (result.goals or 0) * 90 / (result.total_minutes or 1),
                    "xg_per_90": (float(result.xg) if result.xg else 0.0) * 90 / (result.total_minutes or 1),
                    "assists_per_90": (result.assists or 0) * 90 / (result.total_minutes or 1),
                    "xa_per_90": (float(result.xa) if result.xa else 0.0) * 90 / (result.total_minutes or 1)
                }
            
            return {
                "player_id": player_id,
                "season_id": season_id,
                "matches_played": 0,
                "total_minutes": 0,
                "goals": 0,
                "assists": 0,
                "shots": 0,
                "xg": 0.0,
                "key_passes": 0,
                "xa": 0.0,
                "goals_per_90": 0.0,
                "xg_per_90": 0.0,
                "assists_per_90": 0.0,
                "xa_per_90": 0.0
            }
    
    def get_season_top_scorers(self, season_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get top goal scorers for a season
        
        Args:
            season_id: Season ID
            limit: Maximum number of players to return
            
        Returns:
            List of player statistics dictionaries, ordered by goals scored
        """
        with self.session_scope() as session:
            results = (
                session.query(
                    Player.id.label("player_id"),
                    Player.name.label("player_name"),
                    Team.id.label("team_id"),
                    Team.name.label("team_name"),
                    func.sum(PlayerMatchStats.goals).label("goals"),
                    func.sum(PlayerMatchStats.minutes_played).label("minutes_played"),
                    func.sum(PlayerMatchStats.assists).label("assists"),
                    func.sum(PlayerMatchStats.xg).label("xg")
                )
                .join(Player, PlayerMatchStats.player_id == Player.id)
                .join(Team, PlayerMatchStats.team_id == Team.id)
                .join(Match, PlayerMatchStats.match_id == Match.id)
                .filter(Match.season_id == season_id)
                .group_by(Player.id, Player.name, Team.id, Team.name)
                .order_by(desc("goals"))
                .limit(limit)
                .all()
            )
            
            return [
                {
                    "player_id": result.player_id,
                    "player_name": result.player_name,
                    "team_id": result.team_id,
                    "team_name": result.team_name,
                    "goals": result.goals or 0,
                    "minutes_played": result.minutes_played or 0,
                    "assists": result.assists or 0,
                    "xg": float(result.xg) if result.xg else 0.0,
                    "goals_per_90": (result.goals or 0) * 90 / (result.minutes_played or 1),
                    "xg_per_90": (float(result.xg) if result.xg else 0.0) * 90 / (result.minutes_played or 1)
                }
                for result in results
            ]
    
    def get_season_top_assists(self, season_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get top assisters for a season
        
        Args:
            season_id: Season ID
            limit: Maximum number of players to return
            
        Returns:
            List of player statistics dictionaries, ordered by assists
        """
        with self.session_scope() as session:
            results = (
                session.query(
                    Player.id.label("player_id"),
                    Player.name.label("player_name"),
                    Team.id.label("team_id"),
                    Team.name.label("team_name"),
                    func.sum(PlayerMatchStats.assists).label("assists"),
                    func.sum(PlayerMatchStats.minutes_played).label("minutes_played"),
                    func.sum(PlayerMatchStats.goals).label("goals"),
                    func.sum(PlayerMatchStats.xa).label("xa")
                )
                .join(Player, PlayerMatchStats.player_id == Player.id)
                .join(Team, PlayerMatchStats.team_id == Team.id)
                .join(Match, PlayerMatchStats.match_id == Match.id)
                .filter(Match.season_id == season_id)
                .group_by(Player.id, Player.name, Team.id, Team.name)
                .order_by(desc("assists"))
                .limit(limit)
                .all()
            )
            
            return [
                {
                    "player_id": result.player_id,
                    "player_name": result.player_name,
                    "team_id": result.team_id,
                    "team_name": result.team_name,
                    "assists": result.assists or 0,
                    "minutes_played": result.minutes_played or 0,
                    "goals": result.goals or 0,
                    "xa": float(result.xa) if result.xa else 0.0,
                    "assists_per_90": (result.assists or 0) * 90 / (result.minutes_played or 1),
                    "xa_per_90": (float(result.xa) if result.xa else 0.0) * 90 / (result.minutes_played or 1)
                }
                for result in results
            ]
    
    def get_team_head_to_head(
        self, team1_id: str, team2_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get head-to-head matches between two teams
        
        Args:
            team1_id: First team ID
            team2_id: Second team ID
            limit: Maximum number of matches to return
            
        Returns:
            List of match data dictionaries
        """
        with self.session_scope() as session:
            matches = (
                session.query(Match)
                .filter(
                    ((Match.home_team_id == team1_id) & (Match.away_team_id == team2_id)) |
                    ((Match.home_team_id == team2_id) & (Match.away_team_id == team1_id))
                )
                .order_by(desc(Match.date))
                .limit(limit)
                .all()
            )
            
            # Get additional match stats for each match
            result = []
            for match in matches:
                match_stats = (
                    session.query(MatchStats)
                    .filter(MatchStats.match_id == match.id)
                    .first()
                )
                
                home_team = session.query(Team).filter(Team.id == match.home_team_id).first()
                away_team = session.query(Team).filter(Team.id == match.away_team_id).first()
                
                match_data = {
                    "id": match.id,
                    "date": match.date.isoformat() if match.date else None,
                    "home_team_id": match.home_team_id,
                    "away_team_id": match.away_team_id,
                    "home_team_name": home_team.name if home_team else "Unknown",
                    "away_team_name": away_team.name if away_team else "Unknown"
                }
                
                if match_stats:
                    match_data.update({
                        "home_goals": match_stats.home_goals,
                        "away_goals": match_stats.away_goals,
                        "home_xg": match_stats.home_xg,
                        "away_xg": match_stats.away_xg
                    })
                
                result.append(match_data)
            
            return result
    
    def get_player_form(
        self, player_id: str, last_n_matches: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get player form based on recent matches
        
        Args:
            player_id: Player ID
            last_n_matches: Number of recent matches to consider
            
        Returns:
            List of player match statistics dictionaries
        """
        with self.session_scope() as session:
            recent_stats = (
                session.query(
                    PlayerMatchStats,
                    Match.date,
                    Team.name.label("team_name"),
                    func.case(
                        [(Team.id == Match.home_team_id, "home")],
                        else_="away"
                    ).label("venue")
                )
                .join(Match, PlayerMatchStats.match_id == Match.id)
                .join(Team, PlayerMatchStats.team_id == Team.id)
                .filter(PlayerMatchStats.player_id == player_id)
                .order_by(desc(Match.date))
                .limit(last_n_matches)
                .all()
            )
            
            return [
                {
                    "match_id": stats.PlayerMatchStats.match_id,
                    "date": stats.date.isoformat() if stats.date else None,
                    "team_name": stats.team_name,
                    "venue": stats.venue,
                    "minutes_played": stats.PlayerMatchStats.minutes_played,
                    "goals": stats.PlayerMatchStats.goals,
                    "assists": stats.PlayerMatchStats.assists,
                    "shots": stats.PlayerMatchStats.shots,
                    "xg": stats.PlayerMatchStats.xg,
                    "key_passes": stats.PlayerMatchStats.key_passes,
                    "xa": stats.PlayerMatchStats.xa
                }
                for stats in recent_stats
            ]
    
    def get_season_underperformers(self, season_id: str, min_minutes: int = 450) -> List[Dict[str, Any]]:
        """
        Get players underperforming their xG in a season
        
        Args:
            season_id: Season ID
            min_minutes: Minimum minutes played to be included
            
        Returns:
            List of player statistics dictionaries, ordered by xG-goals difference
        """
        with self.session_scope() as session:
            results = (
                session.query(
                    Player.id.label("player_id"),
                    Player.name.label("player_name"),
                    Team.id.label("team_id"),
                    Team.name.label("team_name"),
                    func.sum(PlayerMatchStats.minutes_played).label("minutes_played"),
                    func.sum(PlayerMatchStats.goals).label("goals"),
                    func.sum(PlayerMatchStats.xg).label("xg"),
                    (func.sum(PlayerMatchStats.xg) - func.sum(PlayerMatchStats.goals)).label("xg_diff")
                )
                .join(Player, PlayerMatchStats.player_id == Player.id)
                .join(Team, PlayerMatchStats.team_id == Team.id)
                .join(Match, PlayerMatchStats.match_id == Match.id)
                .filter(Match.season_id == season_id)
                .group_by(Player.id, Player.name, Team.id, Team.name)
                .having(func.sum(PlayerMatchStats.minutes_played) >= min_minutes)
                .order_by(desc("xg_diff"))
                .limit(20)
                .all()
            )
            
            return [
                {
                    "player_id": result.player_id,
                    "player_name": result.player_name,
                    "team_id": result.team_id,
                    "team_name": result.team_name,
                    "minutes_played": result.minutes_played or 0,
                    "goals": result.goals or 0,
                    "xg": float(result.xg) if result.xg else 0.0,
                    "xg_diff": float(result.xg_diff) if result.xg_diff else 0.0,
                    "goals_per_90": (result.goals or 0) * 90 / (result.minutes_played or 1),
                    "xg_per_90": (float(result.xg) if result.xg else 0.0) * 90 / (result.minutes_played or 1)
                }
                for result in results
            ]
    
    def get_season_overperformers(self, season_id: str, min_minutes: int = 450) -> List[Dict[str, Any]]:
        """
        Get players overperforming their xG in a season
        
        Args:
            season_id: Season ID
            min_minutes: Minimum minutes played to be included
            
        Returns:
            List of player statistics dictionaries, ordered by goals-xG difference
        """
        with self.session_scope() as session:
            results = (
                session.query(
                    Player.id.label("player_id"),
                    Player.name.label("player_name"),
                    Team.id.label("team_id"),
                    Team.name.label("team_name"),
                    func.sum(PlayerMatchStats.minutes_played).label("minutes_played"),
                    func.sum(PlayerMatchStats.goals).label("goals"),
                    func.sum(PlayerMatchStats.xg).label("xg"),
                    (func.sum(PlayerMatchStats.goals) - func.sum(PlayerMatchStats.xg)).label("goals_diff")
                )
                .join(Player, PlayerMatchStats.player_id == Player.id)
                .join(Team, PlayerMatchStats.team_id == Team.id)
                .join(Match, PlayerMatchStats.match_id == Match.id)
                .filter(Match.season_id == season_id)
                .group_by(Player.id, Player.name, Team.id, Team.name)
                .having(func.sum(PlayerMatchStats.minutes_played) >= min_minutes)
                .order_by(desc("goals_diff"))
                .limit(20)
                .all()
            )
            
            return [
                {
                    "player_id": result.player_id,
                    "player_name": result.player_name,
                    "team_id": result.team_id,
                    "team_name": result.team_name,
                    "minutes_played": result.minutes_played or 0,
                    "goals": result.goals or 0,
                    "xg": float(result.xg) if result.xg else 0.0,
                    "goals_diff": float(result.goals_diff) if result.goals_diff else 0.0,
                    "goals_per_90": (result.goals or 0) * 90 / (result.minutes_played or 1),
                    "xg_per_90": (float(result.xg) if result.xg else 0.0) * 90 / (result.minutes_played or 1)
                }
                for result in results
            ]
    
    # Batch operations
    
    def bulk_save_players(self, players_data: List[Dict[str, Any]]) -> List[Player]:
        """
        Save multiple players to the database in one transaction
        
        Args:
            players_data: List of player data dictionaries
            
        Returns:
            List of saved Player objects
        """
        players = []
        with self.session_scope() as session:
            for player_data in players_data:
                player_id = player_data.get("id")
                existing_player = session.query(Player).filter(Player.id == player_id).first()
                
                if existing_player:
                    for key, value in player_data.items():
                        if hasattr(existing_player, key):
                            setattr(existing_player, key, value)
                    player = existing_player
                else:
                    player = Player(**player_data)
                    session.add(player)
                
                players.append(player)
            
            session.flush()
            self.logger.info(f"Bulk saved {len(players)} players")
            return players
    
    def bulk_save_teams(self, teams_data: List[Dict[str, Any]]) -> List[Team]:
        """
        Save multiple teams to the database in one transaction
        
        Args:
            teams_data: List of team data dictionaries
            
        Returns:
            List of saved Team objects
        """
        teams = []
        with self.session_scope() as session:
            for team_data in teams_data:
                team_id = team_data.get("id")
                existing_team = session.query(Team).filter(Team.id == team_id).first()
                
                if existing_team:
                    for key, value in team_data.items():
                        if hasattr(existing_team, key):
                            setattr(existing_team, key, value)
                    team = existing_team
                else:
                    team = Team(**team_data)
                    session.add(team)
                
                teams.append(team)
            
            session.flush()
            self.logger.info(f"Bulk saved {len(teams)} teams")
            return teams
    
    def bulk_save_matches(self, matches_data: List[Dict[str, Any]]) -> List[Match]:
        """
        Save multiple matches to the database in one transaction
        
        Args:
            matches_data: List of match data dictionaries
            
        Returns:
            List of saved Match objects
        """
        matches = []
        with self.session_scope() as session:
            for match_data in matches_data:
                match_id = match_data.get("id")
                existing_match = session.query(Match).filter(Match.id == match_id).first()
                
                if existing_match:
                    for key, value in match_data.items():
                        if hasattr(existing_match, key):
                            setattr(existing_match, key, value)
                    match = existing_match
                else:
                    match = Match(**match_data)
                    session.add(match)
                
                matches.append(match)
            
            session.flush()
            self.logger.info(f"Bulk saved {len(matches)} matches")
            return matches
    
    def bulk_save_player_match_stats(
        self, stats_data: List[Dict[str, Any]]
    ) -> List[PlayerMatchStats]:
        """
        Save multiple player match statistics to the database in one transaction
        
        Args:
            stats_data: List of player match statistics data dictionaries
            
        Returns:
            List of saved PlayerMatchStats objects
        """
        stats_objects = []
        with self.session_scope() as session:
            for data in stats_data:
                player_id = data.get("player_id")
                match_id = data.get("match_id")
                
                existing_stats = (
                    session.query(PlayerMatchStats)
                    .filter(
                        PlayerMatchStats.player_id == player_id,
                        PlayerMatchStats.match_id == match_id
                    )
                    .first()
                )
                
                if existing_stats:
                    for key, value in data.items():
                        if hasattr(existing_stats, key):
                            setattr(existing_stats, key, value)
                    stats = existing_stats
                else:
                    stats = PlayerMatchStats(**data)
                    session.add(stats)
                
                stats_objects.append(stats)
            
            session.flush()
            self.logger.info(f"Bulk saved {len(stats_objects)} player match statistics")
            return stats_objects