# src/scraping/scrapers/match_scraper.py

import json
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import time
import urllib.parse

from bs4 import BeautifulSoup
from src.scraping.scrapers.base_scraper import BaseScraper
from config.settings import config


class MatchScraper(BaseScraper):
    """Scraper for football match data from understat.com"""
    
    def __init__(self):
        # Use Selenium since understat.com uses JavaScript to load data
        super().__init__(use_selenium=True)
    
    def scrape(self, league: str, season: str) -> List[Dict[str, Any]]:
        """
        Scrape all matches for a specific league and season
        
        Args:
            league: League identifier (e.g., "EPL", "La_liga")
            season: Season identifier (e.g., "2021")
            
        Returns:
            List of match data dictionaries
        """
        self.logger.info(f"Scraping matches for {league} - {season}")
        
        # Construct URL for the league and season
        url = f"{self.base_url}/league/{league}/{season}"
        
        # Use Selenium to get the page with JavaScript-loaded content
        html_content = self._fetch_with_selenium(url)
        
        # Extract match data from the page
        matches = self._extract_match_data(html_content)
        
        self.logger.info(f"Successfully scraped {len(matches)} matches for {league} - {season}")
        return matches
    
    def scrape_match_details(self, match_id: str) -> Dict[str, Any]:
        """
        Scrape detailed data for a specific match
        
        Args:
            match_id: Unique identifier for the match
            
        Returns:
            Dictionary containing detailed match data
        """
        self.logger.info(f"Scraping details for match {match_id}")
        
        # Construct URL for the match
        url = f"{self.base_url}/match/{match_id}"
        
        # Use Selenium to get the page with JavaScript-loaded content
        html_content = self._fetch_with_selenium(url)
        
        # Extract match details from the page
        match_details = self._extract_match_details(html_content, match_id)
        
        self.logger.info(f"Successfully scraped details for match {match_id}")
        return match_details
    
    def _extract_match_data(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Extract match data from the HTML content
        
        Args:
            html_content: HTML content of the league page
            
        Returns:
            List of match data dictionaries
        """
        # Find the JavaScript variable containing match data
        match_data_pattern = re.compile(r"var\s+datesData\s*=\s*JSON\.parse\s*\('(.+?)'\);")
        match = match_data_pattern.search(html_content)
        
        if not match:
            self.logger.error("Failed to find match data in HTML content")
            return []
        
        # Extract and parse the JSON data
        json_str = match.group(1)
        # Handle escaped quotes and other characters
        json_str = json_str.replace("\\'", "'").replace("\\\\", "\\")
        
        try:
            matches_data = json.loads(json_str)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse match data JSON: {e}")
            return []
        
        # Process and structure the match data
        processed_matches = []
        
        for date, matches in matches_data.items():
            for match in matches:
                processed_match = {
                    "id": match.get("id"),
                    "home_team": match.get("h", {}).get("title"),
                    "away_team": match.get("a", {}).get("title"),
                    "home_team_id": match.get("h", {}).get("id"),
                    "away_team_id": match.get("a", {}).get("id"),
                    "home_goals": match.get("goals", {}).get("h"),
                    "away_goals": match.get("goals", {}).get("a"),
                    "home_xG": match.get("xG", {}).get("h"),
                    "away_xG": match.get("xG", {}).get("a"),
                    "match_date": date,
                    "status": match.get("status"),
                    "league": match.get("league"),
                    "season": match.get("season"),
                    "datetime": match.get("datetime"),
                }
                processed_matches.append(processed_match)
        
        return processed_matches
    
    def _extract_match_details(self, html_content: str, match_id: str) -> Dict[str, Any]:
        """
        Extract detailed match data from the HTML content
        
        Args:
            html_content: HTML content of the match page
            match_id: Match identifier
            
        Returns:
            Dictionary containing detailed match data
        """
        soup = self._parse_html(html_content)
        
        # Extract match info
        match_info = {}
        
        # Find the JavaScript variables containing match data
        script_tags = soup.find_all("script")
        
        # Extract shots data
        shots_pattern = re.compile(r"var\s+shotsData\s*=\s*JSON\.parse\s*\('(.+?)'\);")
        roster_pattern = re.compile(r"var\s+rostersData\s*=\s*JSON\.parse\s*\('(.+?)'\);")
        stats_pattern = re.compile(r"var\s+statisticsData\s*=\s*JSON\.parse\s*\('(.+?)'\);")
        
        for script in script_tags:
            if script.string:
                # Extract shots data
                shots_match = shots_pattern.search(script.string)
                if shots_match:
                    json_str = shots_match.group(1).replace("\\'", "'").replace("\\\\", "\\")
                    try:
                        match_info["shots"] = json.loads(json_str)
                    except json.JSONDecodeError:
                        self.logger.error(f"Failed to parse shots data for match {match_id}")
                
                # Extract roster data
                roster_match = roster_pattern.search(script.string)
                if roster_match:
                    json_str = roster_match.group(1).replace("\\'", "'").replace("\\\\", "\\")
                    try:
                        match_info["rosters"] = json.loads(json_str)
                    except json.JSONDecodeError:
                        self.logger.error(f"Failed to parse roster data for match {match_id}")
                
                # Extract statistics data
                stats_match = stats_pattern.search(script.string)
                if stats_match:
                    json_str = stats_match.group(1).replace("\\'", "'").replace("\\\\", "\\")
                    try:
                        match_info["statistics"] = json.loads(json_str)
                    except json.JSONDecodeError:
                        self.logger.error(f"Failed to parse statistics data for match {match_id}")
        
        # Add match_id to the result
        match_info["match_id"] = match_id
        
        # Add timestamp for when this data was scraped
        match_info["scraped_at"] = datetime.now().isoformat()
        
        return match_info