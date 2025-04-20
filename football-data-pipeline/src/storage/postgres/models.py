# src/storage/postgres/models.py

from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, 
    ForeignKey, Table, MetaData, create_engine, JSON,
    UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class League(Base):
    """League table for storing league information"""
    __tablename__ = 'leagues'
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    
    # Relationships
    seasons = relationship("Season", back_populates="league")
    
    # Indexes
    __table_args__ = (
        Index('idx_leagues_name', name),
        Index('idx_leagues_country', country),
    )
    
    def __repr__(self):
        return f"<League(id='{self.id}', name='{self.name}', country='{self.country}')>"


class Season(Base):
    """Season table for storing season information"""
    __tablename__ = 'seasons'
    
    id = Column(String, primary_key=True)
    year = Column(String, nullable=False)
    league_id = Column(String, ForeignKey('leagues.id'), nullable=False)
    is_current = Column(Boolean, default=False)
    
    # Relationships
    league = relationship("League", back_populates="seasons")
    matches = relationship("Match", back_populates="season")
    team_stats = relationship("TeamSeasonStats", back_populates="season")
    
    # Indexes
    __table_args__ = (
        Index('idx_seasons_league_year', league_id, year),
        Index('idx_seasons_is_current', is_current),
    )
    
    def __repr__(self):
        return f"<Season(id='{self.id}', year='{self.year}', league_id='{self.league_id}')>"


class Team(Base):
    """Team table for storing team information"""
    __tablename__ = 'teams'
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    short_name = Column(String)
    
    # Relationships
    home_matches = relationship("Match", foreign_keys="Match.home_team_id", back_populates="home_team")
    away_matches = relationship("Match", foreign_keys="Match.away_team_id", back_populates="away_team")
    team_stats = relationship("TeamSeasonStats", back_populates="team")
    players = relationship("Player", back_populates="team")
    
    # Indexes
    __table_args__ = (
        Index('idx_teams_name', name),
    )
    
    def __repr__(self):
        return f"<Team(id='{self.id}', name='{self.name}')>"


class Player(Base):
    """Player table for storing player information"""
    __tablename__ = 'players'
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    position = Column(String)
    team_id = Column(String, ForeignKey('teams.id'))
    
    # Relationships
    team = relationship("Team", back_populates="players")
    match_stats = relationship("PlayerMatchStats", back_populates="player")
    
    # Indexes
    __table_args__ = (
        Index('idx_players_name', name),
        Index('idx_players_team', team_id),
    )
    
    def __repr__(self):
        return f"<Player(id='{self.id}', name='{self.name}', team_id='{self.team_id}')>"


class Match(Base):
    """Match table for storing match information"""
    __tablename__ = 'matches'
    
    id = Column(String, primary_key=True)
    home_team_id = Column(String, ForeignKey('teams.id'), nullable=False)
    away_team_id = Column(String, ForeignKey('teams.id'), nullable=False)
    season_id = Column(String, ForeignKey('seasons.id'), nullable=False)
    date = Column(DateTime, nullable=False)
    status = Column(String, nullable=False)  # scheduled, played, postponed, cancelled
    
    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_matches")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_matches")
    season = relationship("Season", back_populates="matches")
    stats = relationship("MatchStats", back_populates="match", uselist=False)
    player_stats = relationship("PlayerMatchStats", back_populates="match")
    events = relationship("MatchEvent", back_populates="match")
    
    # Indexes
    __table_args__ = (
        Index('idx_matches_date', date),
        Index('idx_matches_teams', home_team_id, away_team_id),
        Index('idx_matches_season', season_id),
        Index('idx_matches_status', status),
    )
    
    def __repr__(self):
        return f"<Match(id='{self.id}', home_team_id='{self.home_team_id}', away_team_id='{self.away_team_id}', date='{self.date}')>"


class MatchStats(Base):
    """MatchStats table for storing detailed match statistics"""
    __tablename__ = 'match_stats'
    
    id = Column(String, primary_key=True)
    match_id = Column(String, ForeignKey('matches.id'), nullable=False, unique=True)
    home_goals = Column(Integer, nullable=False, default=0)
    away_goals = Column(Integer, nullable=False, default=0)
    home_xg = Column(Float)
    away_xg = Column(Float)
    home_shots = Column(Integer)
    away_shots = Column(Integer)
    home_shots_on_target = Column(Integer)
    away_shots_on_target = Column(Integer)
    home_corners = Column(Integer)
    away_corners = Column(Integer)
    home_fouls = Column(Integer)
    away_fouls = Column(Integer)
    home_yellow_cards = Column(Integer)
    away_yellow_cards = Column(Integer)
    home_red_cards = Column(Integer)
    away_red_cards = Column(Integer)
    home_possession = Column(Float)
    away_possession = Column(Float)
    additional_stats = Column(JSON)  # For other stats not in standard columns
    
    # Relationships
    match = relationship("Match", back_populates="stats")
    
    # Indexes
    __table_args__ = (
        Index('idx_match_stats_match', match_id),
    )
    
    def __repr__(self):
        return f"<MatchStats(match_id='{self.match_id}', home_goals={self.home_goals}, away_goals={self.away_goals})>"


class PlayerMatchStats(Base):
    """PlayerMatchStats table for storing player performance in matches"""
    __tablename__ = 'player_match_stats'
    
    id = Column(String, primary_key=True)
    player_id = Column(String, ForeignKey('players.id'), nullable=False)
    match_id = Column(String, ForeignKey('matches.id'), nullable=False)
    minutes_played = Column(Integer)
    goals = Column(Integer, default=0)
    assists = Column(Integer, default=0)
    shots = Column(Integer)
    shots_on_target = Column(Integer)
    xg = Column(Float)
    xa = Column(Float)
    passes_completed = Column(Integer)
    passes_attempted = Column(Integer)
    key_passes = Column(Integer)
    tackles = Column(Integer)
    interceptions = Column(Integer)
    additional_stats = Column(JSON)  # For other stats not in standard columns
    
    # Relationships
    player = relationship("Player", back_populates="match_stats")
    match = relationship("Match", back_populates="player_stats")
    
    # Indexes and constraints
    __table_args__ = (
        UniqueConstraint('player_id', 'match_id', name='uq_player_match'),
        Index('idx_player_match_stats_player', player_id),
        Index('idx_player_match_stats_match', match_id),
    )
    
    def __repr__(self):
        return f"<PlayerMatchStats(player_id='{self.player_id}', match_id='{self.match_id}', goals={self.goals})>"


class TeamSeasonStats(Base):
    """TeamSeasonStats table for storing team statistics for a season"""
    __tablename__ = 'team_season_stats'
    
    id = Column(String, primary_key=True)
    team_id = Column(String, ForeignKey('teams.id'), nullable=False)
    season_id = Column(String, ForeignKey('seasons.id'), nullable=False)
    matches_played = Column(Integer, default=0)
    wins = Column(Integer, default=0)
    draws = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    goals_for = Column(Integer, default=0)
    goals_against = Column(Integer, default=0)
    points = Column(Integer, default=0)
    xg_for = Column(Float, default=0.0)
    xg_against = Column(Float, default=0.0)
    clean_sheets = Column(Integer, default=0)
    additional_stats = Column(JSON)  # For other stats not in standard columns
    
    # Relationships
    team = relationship("Team", back_populates="team_stats")
    season = relationship("Season", back_populates="team_stats")
    
    # Indexes and constraints
    __table_args__ = (
        UniqueConstraint('team_id', 'season_id', name='uq_team_season'),
        Index('idx_team_season_stats_team', team_id),
        Index('idx_team_season_stats_season', season_id),
    )
    
    def __repr__(self):
        return f"<TeamSeasonStats(team_id='{self.team_id}', season_id='{self.season_id}', points={self.points})>"


class MatchEvent(Base):
    """MatchEvent table for storing events during a match"""
    __tablename__ = 'match_events'
    
    id = Column(String, primary_key=True)
    match_id = Column(String, ForeignKey('matches.id'), nullable=False)
    player_id = Column(String, ForeignKey('players.id'))
    team_id = Column(String, ForeignKey('teams.id'), nullable=False)
    event_type = Column(String, nullable=False)  # goal, card, substitution, etc.
    minute = Column(Integer, nullable=False)
    second = Column(Integer, default=0)
    extra_info = Column(JSON)  # Additional event details
    
    # Relationships
    match = relationship("Match", back_populates="events")
    
    # Indexes
    __table_args__ = (
        Index('idx_match_events_match', match_id),
        Index('idx_match_events_player', player_id),
        Index('idx_match_events_team', team_id),
        Index('idx_match_events_type', event_type),
    )
    
    def __repr__(self):
        return f"<MatchEvent(match_id='{self.match_id}', event_type='{self.event_type}', minute={self.minute})>"