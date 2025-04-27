# src/streaming/match_streaming.py
import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.extraction.understat.scraper import UnderstatScraper
from src.streaming.redpanda_producer import RedpandaProducer, create_producer_from_config
from src.utils.config import Config

logger = logging.getLogger(__name__)

class MatchStreamingService:
    """Service for streaming football match data from understat."""
    
    def __init__(
        self, 
        producer: RedpandaProducer,
        scraper: UnderstatScraper,
        match_topic: str = "football-matches",
        player_stats_topic: str = "football-player-stats",
        shot_topic: str = "football-shots",
        poll_interval: int = 3600,  # 1 hour default
    ):
        """Initialize the match streaming service.
        
        Args:
            producer: RedpandaProducer instance
            scraper: UnderstatScraper instance
            match_topic: Topic for match data
            player_stats_topic: Topic for player statistics
            shot_topic: Topic for shot data
            poll_interval: Interval between polls in seconds
        """
        self.producer = producer
        self.scraper = scraper
        self.match_topic = match_topic
        self.player_stats_topic = player_stats_topic
        self.shot_topic = shot_topic
        self.poll_interval = poll_interval
        self.last_processed_dates = {}
        
    def stream_recent_matches(self, league: str, season: Optional[str] = None) -> None:
        """Stream recent matches for a given league and season.
        
        Args:
            league: League name (e.g., "EPL", "La_liga")
            season: Season (e.g., "2023") or None for current season
        """
        # Get current season if not specified
        if not season:
            current_year = datetime.now().year
            month = datetime.now().month
            # If it's before August, use previous year as season start
            season = str(current_year - 1) if month < 8 else str(current_year)
        
        # Get key for tracking last processed date
        key = f"{league}_{season}"
        
        # Get date to check from (either last processed or 7 days ago)
        if key in self.last_processed_dates:
            from_date = self.last_processed_dates[key]
        else:
            from_date = datetime.now() - timedelta(days=7)
        
        logger.info(f"Fetching matches for {league} {season} from {from_date}")
        
        # Get matches from the API
        try:
            matches = self.scraper.get_league_matches(league, season)
            
            # Filter for matches after from_date
            recent_matches = [
                match for match in matches 
                if datetime.strptime(match['date'], "%Y-%m-%d %H:%M:%S") > from_date
            ]
            
            if not recent_matches:
                logger.info(f"No new matches found for {league} {season}")
                return
                
            logger.info(f"Found {len(recent_matches)} recent matches for {league} {season}")
            
            # Produce match data to topic
            self.producer.produce_batch(self.match_topic, recent_matches, key_field="id")
            
            # Get detailed data for each match
            for match in recent_matches:
                match_id = match['id']
                
                # Get player stats
                try:
                    player_stats = self.scraper.get_match_player_stats(match_id)
                    if player_stats:
                        # Add match metadata to each player stat
                        for player_id, stats in player_stats.items():
                            stats['match_id'] = match_id
                            stats['league'] = league
                            stats['season'] = season
                            stats['player_id'] = player_id
                            self.producer.produce(
                                self.player_stats_topic, 
                                stats, 
                                key=f"{match_id}_{player_id}"
                            )
                except Exception as e:
                    logger.error(f"Error fetching player stats for match {match_id}: {e}")
                
                # Get shot data
                try:
                    shot_data = self.scraper.get_match_shots(match_id)
                    if shot_data:
                        # Flatten and add match metadata to each shot
                        for team, shots in shot_data.items():
                            for shot in shots:
                                shot['match_id'] = match_id
                                shot['team'] = team
                                shot['league'] = league
                                shot['season'] = season
                                self.producer.produce(
                                    self.shot_topic,
                                    shot,
                                    key=shot.get('id', f"{match_id}_{shot.get('player')}")
                                )
                except Exception as e:
                    logger.error(f"Error fetching shot data for match {match_id}: {e}")
                
                time.sleep(1)  # Brief pause to avoid rate limiting
                
            # Update last processed date
            self.last_processed_dates[key] = datetime.now()
            
        except Exception as e:
            logger.error(f"Error streaming matches for {league} {season}: {e}")
    
    def run_streaming_service(self, leagues: List[str], seasons: Optional[List[str]] = None) -> None:
        """Run the streaming service for multiple leagues and seasons.
        
        Args:
            leagues: List of leagues to stream
            seasons: List of seasons to stream or None for current season
        """
        if not seasons:
            current_year = datetime.now().year
            month = datetime.now().month
            # If it's before August, use previous year as season start
            current_season = str(current_year - 1) if month < 8 else str(current_year)
            seasons = [current_season]
            
        try:
            logger.info(f"Starting match streaming service for leagues: {', '.join(leagues)}")
            logger.info(f"Streaming seasons: {', '.join(seasons)}")
            
            while True:
                for league in leagues:
                    for season in seasons:
                        try:
                            self.stream_recent_matches(league, season)
                        except Exception as e:
                            logger.error(f"Error streaming {league} {season}: {e}")
                
                logger.info(f"Completed streaming cycle. Waiting {self.poll_interval} seconds until next poll.")
                time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            logger.info("Streaming service stopped by user")
        finally:
            self.producer.close()


def main():
    """Main function to run the streaming service."""
    parser = argparse.ArgumentParser(description="Football Match Streaming Service")
    parser.add_argument("--config", type=str, default="config/config.yaml", help="Path to configuration file")
    parser.add_argument("--leagues", type=str, nargs="+", default=["EPL"], help="Leagues to stream")
    parser.add_argument("--seasons", type=str, nargs="+", help="Seasons to stream (default