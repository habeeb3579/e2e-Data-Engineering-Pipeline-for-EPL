import json
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
import datetime
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.utils.config import ScraperConfig, ConfigManager

logger = logging.getLogger(__name__)

config_manager = ConfigManager("/workspaces/e2e-Data-Engineering-Pipeline-for-EPL/football-data-pipeline/config/config.yaml")
pipeline_config = config_manager.get_config()
scraper_config = pipeline_config.scraper

class WebScraperFactory:
    """Factory for creating scrapers based on configured method (bs4 or selenium)"""
    
    @staticmethod
    def create_scraper(config: ScraperConfig):
        if config.scraper_method == "selenium":
            return SeleniumScraper(config)
        else:
            return BeautifulSoupScraper(config)

class BaseScraper(ABC):
    """Base abstract class for scrapers"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.base_url = "https://understat.com"
    
    @abstractmethod
    def get_page_content(self, url: str) -> str:
        """Get the HTML content from a URL"""
        pass
    
    def extract_data_from_script(self, content: Union[str, BeautifulSoup], var_name: str) -> Dict:
        """Extract JSON data from script tag containing a specific variable"""
        if isinstance(content, BeautifulSoup):
            soup = content
        else:
            soup = BeautifulSoup(content, "html.parser")
            
        pattern = re.compile(f"var {var_name} = JSON.parse\('(.*?)'\);", re.DOTALL)
        
        for script in soup.find_all("script"):
            if script.string and var_name in script.string:
                match = pattern.search(script.string)
                if match:
                    # Handle escaped quotes and other special characters
                    json_data = match.group(1).encode('utf-8').decode('unicode_escape')
                    return json.loads(json_data)
        
        logger.error(f"Could not find {var_name} in the page")
        return {}

class BeautifulSoupScraper(BaseScraper):
    """Scraper implementation using BeautifulSoup and requests"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        self.session = requests.Session()
        
        # Configure proxy if specified
        if self.config.use_proxy and self.config.proxy_url:
            self.session.proxies = {
                "http": self.config.proxy_url,
                "https": self.config.proxy_url
            }
    
    def get_page_content(self, url: str) -> str:
        """Get page content using requests"""
        logger.info(f"Making request to {url} with BeautifulSoup")
        
        try:
            response = self.session.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            })
            response.raise_for_status()
            
            # Add delay to avoid hitting rate limits
            time.sleep(self.config.request_delay)
            
            return response.content
        except requests.RequestException as e:
            logger.error(f"Error making request to {url}: {e}")
            raise

class SeleniumScraper(BaseScraper):
    """Scraper implementation using Selenium WebDriver"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
        
        # Configure Chrome options
        chrome_options = Options()
        if config.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Configure proxy if specified
        if self.config.use_proxy and self.config.proxy_url:
            chrome_options.add_argument(f"--proxy-server={self.config.proxy_url}")
        
        # Initialize WebDriver
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
    
    def get_page_content(self, url: str) -> str:
        """Get page content using Selenium"""
        logger.info(f"Making request to {url} with Selenium")
        
        try:
            self.driver.get(url)
            
            # Wait for page to load completely
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "script"))
            )
            
            # Add delay to avoid hitting rate limits
            time.sleep(self.config.request_delay)
            
            return self.driver.page_source
        except Exception as e:
            logger.error(f"Error making request to {url} with Selenium: {e}")
            raise
    
    def __del__(self):
        """Close the browser when the scraper is deleted"""
        if hasattr(self, 'driver'):
            try:
                self.driver.quit()
            except:
                pass

class BaseUnderstatScraper(ABC):
    """Base class for scraping data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.base_url = "https://understat.com"
        self.scraper = WebScraperFactory.create_scraper(config)
    
    def _make_request(self, url: str) -> BeautifulSoup:
        """Make a request to the specified URL and return a BeautifulSoup object"""
        content = self.scraper.get_page_content(url)
        return BeautifulSoup(content, "html.parser")
    
    def _extract_data_from_script(self, soup: BeautifulSoup, var_name: str) -> Dict:
        """Extract JSON data from a script tag containing a specific variable"""
        return self.scraper.extract_data_from_script(soup, var_name)
    
    @abstractmethod
    def scrape(self) -> List[Dict]:
        """Scrape data from understat.com"""
        pass


class LeagueScraper(BaseUnderstatScraper):
    """Scraper for league data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
    
    def scrape(self) -> List[Dict]:
        """Scrape league data for specified leagues and seasons"""
        leagues_data = []
        
        for league in self.config.leagues:
            for season in self.config.seasons:
                logger.info(f"Scraping data for {league} - {season}")
                
                # Build the URL for the league and season
                url = f"{self.base_url}/league/{league}/{season}"
                
                # Make the request
                soup = self._make_request(url)
                
                # Extract league data
                league_data = self._extract_data_from_script(soup, "teamsData")
                
                # Add metadata
                for team_id, team_data in league_data.items():
                    team_data["league"] = league
                    team_data["season"] = season
                    team_data["team_id"] = team_id
                
                leagues_data.append({
                    "league": league,
                    "season": season,
                    "teams": league_data
                })
        
        return leagues_data


class MatchScraper(BaseUnderstatScraper):
    """Scraper for match data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
    
    def scrape_match(self, match_id: str) -> Dict:
        """Scrape data for a specific match"""
        url = f"{self.base_url}/match/{match_id}"
        soup = self._make_request(url)
        
        # Extract match data
        match_data = self._extract_data_from_script(soup, "matchData")
        roster_data = self._extract_data_from_script(soup, "rostersData")
        shots_data = self._extract_data_from_script(soup, "shotsData")
        
        return {
            "match_id": match_id,
            "match_data": match_data,
            "roster_data": roster_data,
            "shots_data": shots_data
        }
    
    def scrape(self) -> List[Dict]:
        """Scrape match data for matches in the specified leagues and seasons"""
        # First, get a list of matches from the league scraper
        league_scraper = LeagueScraper(self.config)
        leagues_data = league_scraper.scrape()
        
        matches_data = []
        
        for league_data in leagues_data:
            league = league_data["league"]
            season = league_data["season"]
            
            url = f"{self.base_url}/league/{league}/{season}"
            soup = self._make_request(url)
            
            # Extract matches data
            dates_data = self._extract_data_from_script(soup, "datesData")
            
            for date, date_matches in dates_data.items():
                for match in date_matches:
                    match_id = match.get("id")
                    
                    if match_id:
                        logger.info(f"Scraping match {match_id}")
                        match_data = self.scrape_match(match_id)
                        match_data["league"] = league
                        match_data["season"] = season
                        match_data["date"] = date
                        matches_data.append(match_data)
        
        return matches_data
    
    def check_for_new_matches(self, since_date: str = None) -> List[str]:
        """Check for new matches since the specified date"""
        if not since_date:
            # Default to last 7 days if no date provided
            since_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
        new_match_ids = []
        
        for league in self.config.leagues:
            for season in self.config.seasons:
                # Only check current season for new matches
                if season == self.config.current_season:
                    url = f"{self.base_url}/league/{league}/{season}"
                    soup = self._make_request(url)
                    
                    # Extract matches data
                    dates_data = self._extract_data_from_script(soup, "datesData")
                    
                    for date, date_matches in dates_data.items():
                        if date >= since_date:
                            for match in date_matches:
                                match_id = match.get("id")
                                if match_id:
                                    new_match_ids.append(match_id)
        
        return new_match_ids


class PlayerScraper(BaseUnderstatScraper):
    """Scraper for player data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
    
    def scrape_player(self, player_id: str) -> Dict:
        """Scrape data for a specific player"""
        url = f"{self.base_url}/player/{player_id}"
        soup = self._make_request(url)
        
        # Extract player data
        player_data = self._extract_data_from_script(soup, "playersData")
        grouped_data = self._extract_data_from_script(soup, "groupsData")
        matches_data = self._extract_data_from_script(soup, "matchesData")
        
        return {
            "player_id": player_id,
            "player_data": player_data,
            "grouped_data": grouped_data,
            "matches_data": matches_data
        }
    
    def scrape(self) -> List[Dict]:
        """Scrape player data for players in the specified leagues and seasons"""
        # First, get all teams from the league scraper
        league_scraper = LeagueScraper(self.config)
        leagues_data = league_scraper.scrape()
        
        players_data = []
        player_ids = set()  # To avoid duplicate players
        
        for league_data in leagues_data:
            teams = league_data.get("teams", {})
            
            for team_id, team_data in teams.items():
                # Get squad players
                url = f"{self.base_url}/team/{team_id}"
                soup = self._make_request(url)
                
                # Extract squad data
                squad_data = self._extract_data_from_script(soup, "playersData")
                
                for player in squad_data:
                    player_id = player.get("id")
                    
                    if player_id and player_id not in player_ids:
                        logger.info(f"Scraping player {player_id}")
                        player_data = self.scrape_player(player_id)
                        players_data.append(player_data)
                        player_ids.add(player_id)
        
        return players_data


class TeamScraper(BaseUnderstatScraper):
    """Scraper for team data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
    
    def scrape_team(self, team_id: str) -> Dict:
        """Scrape data for a specific team"""
        url = f"{self.base_url}/team/{team_id}"
        soup = self._make_request(url)
        
        # Extract team data
        team_data = self._extract_data_from_script(soup, "teamData")
        players_data = self._extract_data_from_script(soup, "playersData")
        dates_data = self._extract_data_from_script(soup, "datesData")
        
        return {
            "team_id": team_id,
            "team_data": team_data,
            "players_data": players_data,
            "dates_data": dates_data
        }
    
    def scrape(self) -> List[Dict]:
        """Scrape team data for teams in the specified leagues and seasons"""
        # First, get all teams from the league scraper
        league_scraper = LeagueScraper(self.config)
        leagues_data = league_scraper.scrape()
        
        teams_data = []
        team_ids = set()  # To avoid duplicate teams
        
        for league_data in leagues_data:
            teams = league_data.get("teams", {})
            
            for team_id in teams.keys():
                if team_id not in team_ids:
                    logger.info(f"Scraping team {team_id}")
                    team_data = self.scrape_team(team_id)
                    team_data["league"] = league_data["league"]
                    team_data["season"] = league_data["season"]
                    teams_data.append(team_data)
                    team_ids.add(team_id)
        
        return teams_data


class UnderstatScraperFactory:
    """Factory class for creating understat scrapers"""
    
    @staticmethod
    def create_scraper(scraper_type: str, config: ScraperConfig) -> BaseUnderstatScraper:
        """Create a scraper of the specified type"""
        if scraper_type == "league":
            return LeagueScraper(config)
        elif scraper_type == "match":
            return MatchScraper(config)
        elif scraper_type == "player":
            return PlayerScraper(config)
        elif scraper_type == "team":
            return TeamScraper(config)
        else:
            raise ValueError(f"Unknown scraper type: {scraper_type}")