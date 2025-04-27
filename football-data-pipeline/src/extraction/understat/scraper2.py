import json
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
import datetime
from datetime import timedelta
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

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load configuration
config_manager = ConfigManager("/workspaces/e2e-Data-Engineering-Pipeline-for-EPL/football-data-pipeline/config/config.yaml")
pipeline_config = config_manager.get_config()
scraper_config = pipeline_config.scraper

class WebScraperFactory:
    """Factory for creating scrapers based on configured method (bs4 or selenium)"""
    
    @staticmethod
    def create_scraper(config: ScraperConfig):
        logger.info(f"Creating scraper with method: {config.scraper_method}")
        if config.scraper_method == "selenium":
            try:
                logger.info("Attempting to use Selenium scraper")
                return SeleniumScraper(config)
            except Exception as e:
                logger.warning(f"Failed to initialize Selenium: {e}")
                logger.warning("Falling back to BeautifulSoup scraper")
                return BeautifulSoupScraper(config)
        else:
            logger.info("Using BeautifulSoup scraper")
            return BeautifulSoupScraper(config)

class BaseScraper(ABC):
    """Base abstract class for scrapers"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.base_url = "https://understat.com"
        logger.info(f"Initialized {self.__class__.__name__}")
    
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

        for script in soup.find_all("script"):
            script_text = script.string
            if not script_text:
                continue

            # Search if this script contains the variable name
            if var_name in script_text:
                # Try to find any JSON.parse(...) in the script
                match = re.search(r"JSON\.parse\('(.+?)'\)", script_text, re.DOTALL)
                if match:
                    escaped_str = match.group(1)
                    try:
                        decoded_str = bytes(escaped_str, "utf-8").decode("unicode_escape")
                        json_obj = json.loads(decoded_str)
                        return json_obj
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode JSON for {var_name}: {e}")
                        return {}

        logger.error(f"Could not find {var_name} in any script block")
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
            logger.info(f"Using proxy: {self.config.proxy_url}")
    
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
            logger.info("Selenium running in headless mode")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Configure proxy if specified
        if self.config.use_proxy and self.config.proxy_url:
            chrome_options.add_argument(f"--proxy-server={self.config.proxy_url}")
            logger.info(f"Using proxy: {self.config.proxy_url}")
        
        # Initialize WebDriver
        try:
            logger.info("Initializing Chrome WebDriver")
            self.driver = webdriver.Chrome(
                #service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            raise
    
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
                logger.info("Closing Chrome WebDriver")
                self.driver.quit()
            except Exception as e:
                logger.error(f"Error closing Chrome WebDriver: {e}")

class BaseUnderstatScraper(ABC):
    """Base class for scraping data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.base_url = "https://understat.com"
        self.scraper = WebScraperFactory.create_scraper(config)
        logger.info(f"Created {type(self.scraper).__name__} for {self.__class__.__name__}")
    
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
                
                logger.info(f"Successfully scraped data for {league} - {season}")
        
        return leagues_data


class MatchScraper(BaseUnderstatScraper):
    """Scraper for match data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
    
    def scrape_match(self, match_id: str) -> Dict:
        """Scrape data for a specific match"""
        url = f"{self.base_url}/match/{match_id}"
        logger.info(f"Scraping match {match_id} from {url}")
        
        soup = self._make_request(url)
        
        # Extract match data
        match_data = self._extract_data_from_script(soup, "match_info")
        roster_data = self._extract_data_from_script(soup, "rostersData")
        shots_data = self._extract_data_from_script(soup, "shotsData")
        
        logger.info(f"Successfully scraped match {match_id}")
        
        return {
            "match_id": match_id,
            "match_data": match_data,
            "roster_data": roster_data,
            "shots_data": shots_data
        }
    
    
    def scrape(self) -> List[Dict]:
        """Scrape match data for matches in the specified leagues and seasons"""
        logger.info("Using league scraper to get matches list")
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
            
            if not isinstance(dates_data, list):
                logger.error(f"Unexpected format for datesData from {url}")
                continue
            
            for match in dates_data:
                match_id = match.get("id")
                match_date = match.get("datetime", "").split(" ")[0]  # Only keep YYYY-MM-DD part
                
                if match_id:
                    logger.info(f"Scraping match {match_id}")
                    match_data = self.scrape_match(match_id)
                    match_data["league"] = league
                    match_data["season"] = season
                    match_data["date"] = match_date
                    matches_data.append(match_data)

        logger.info(f"Successfully scraped {len(matches_data)} matches")
        return matches_data

    
    def check_for_new_matches(self, since_date: str = None) -> List[str]:
        """Check for new matches since the specified date"""
        if not since_date:
            # Default to last 7 days if no date provided
            since_date = (datetime.datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
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
        
        logger.info(f"Found {len(new_match_ids)} new matches since {since_date}")
        return new_match_ids


class PlayerScraper(BaseUnderstatScraper):
    """Scraper for player data from understat.com"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
    
    def scrape_player(self, player_id: str) -> Dict:
        """Scrape data for a specific player"""
        url = f"{self.base_url}/player/{player_id}"
        logger.info(f"Scraping player {player_id} from {url}")
        
        soup = self._make_request(url)
        
        # Extract player data
        player_data = self._extract_data_from_script(soup, "playersData")
        grouped_data = self._extract_data_from_script(soup, "groupsData")
        matches_data = self._extract_data_from_script(soup, "matchesData")
        
        logger.info(f"Successfully scraped player {player_id}")
        
        return {
            "player_id": player_id,
            "player_data": player_data,
            "grouped_data": grouped_data,
            "matches_data": matches_data
        }
    
    def scrape(self) -> List[Dict]:
        """Scrape player data for players in the specified leagues and seasons"""
        # First, get all teams from the league scraper
        logger.info("Using league scraper to get teams and players list")
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
        
        logger.info(f"Successfully scraped {len(players_data)} players")
        return players_data


class TeamScraper(BaseUnderstatScraper):
    """Scraper for team data from understat.com using team name instead of team ID"""
    
    def __init__(self, config: ScraperConfig):
        super().__init__(config)
    
    def scrape_team(self, team_name: str) -> Dict:
        """Scrape data for a specific team by name"""
        url = f"{self.base_url}/team/{team_name}"
        logger.info(f"Scraping team '{team_name}' from {url}")
        
        soup = self._make_request(url)
        
        # Extract team data
        #team_data = self._extract_data_from_script(soup, "teamsData")
        team_data = self._extract_data_from_script(soup, "statisticsData")
        players_data = self._extract_data_from_script(soup, "playersData")
        dates_data = self._extract_data_from_script(soup, "datesData")
        
        logger.info(f"Successfully scraped team '{team_name}'")
        
        return {
            "team_name": team_name,
            "team_data": team_data,
            "players_data": players_data,
            "dates_data": dates_data
        }

    def scrape(self) -> List[Dict]:
        """Scrape team data for teams in the specified leagues and seasons"""
        logger.info("Using league scraper to get teams list")
        league_scraper = LeagueScraper(self.config)
        leagues_data = league_scraper.scrape()
        
        teams_data = []
        team_names = set()  # To avoid duplicate teams
        
        for league_data in leagues_data:
            teams = league_data.get("teams", {})
            
            for team_id, team_info in teams.items():
                team_name = team_info.get("title")
                
                if team_name and team_name not in team_names:
                    logger.info(f"Scraping team '{team_name}'")
                    team_data = self.scrape_team(team_name)
                    team_data["league"] = league_data["league"]
                    team_data["season"] = league_data["season"]
                    teams_data.append(team_data)
                    team_names.add(team_name)
        
        logger.info(f"Successfully scraped {len(teams_data)} teams")
        return teams_data


class UnderstatScraperFactory:
    """Factory class for creating understat scrapers"""
    
    @staticmethod
    def create_scraper(scraper_type: str, config: ScraperConfig) -> BaseUnderstatScraper:
        """Create a scraper of the specified type"""
        logger.info(f"Creating {scraper_type} scraper with method: {config.scraper_method}")
        
        if scraper_type == "league":
            return LeagueScraper(config)
        elif scraper_type == "match":
            return MatchScraper(config)
        elif scraper_type == "player":
            return PlayerScraper(config)
        elif scraper_type == "team":
            return TeamScraper(config)
        else:
            logger.error(f"Unknown scraper type: {scraper_type}")
            raise ValueError(f"Unknown scraper type: {scraper_type}")