# src/scraping/scrapers/base_scraper.py

import time
import logging
import requests
import random
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from config.settings import config
from src.utils.logging_utils import get_logger
from src.scraping.utils.proxies import ProxyManager
from src.scraping.utils.rate_limiter import RateLimiter


class BaseScraper(ABC):
    """Base class for all scrapers with common functionality"""
    
    def __init__(self, use_selenium: bool = False):
        self.logger = get_logger(self.__class__.__name__)
        self.base_url = config.scraper.base_url
        self.headers = config.scraper.headers
        self.timeout = config.scraper.request_timeout
        
        # Rate limiting
        self.rate_limiter = RateLimiter(
            requests_per_minute=config.scraper.rate_limit
        )
        
        # Proxy management (optional)
        self.proxy_manager = None
        if config.scraper.use_proxies:
            self.proxy_manager = ProxyManager()
        
        # Selenium setup (optional)
        self.driver = None
        if use_selenium:
            self._setup_selenium()
    
    def _setup_selenium(self):
        """Set up Selenium webdriver"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
    
    def _get_request_headers(self) -> Dict[str, str]:
        """Get headers for the request with optional user agent rotation"""
        headers = self.headers.copy()
        
        if config.scraper.user_agent_rotation:
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
            ]
            headers["User-Agent"] = random.choice(user_agents)
            
        return headers
    
    def _get_proxy(self) -> Optional[Dict[str, str]]:
        """Get proxy configuration if enabled"""
        if self.proxy_manager:
            return self.proxy_manager.get_proxy()
        return None
    
    def _make_request(self, url: str, params: Dict[str, Any] = None) -> requests.Response:
        """Make an HTTP request with rate limiting and proxy rotation"""
        self.rate_limiter.wait()
        
        headers = self._get_request_headers()
        proxies = self._get_proxy()
        
        self.logger.info(f"Making request to {url}")
        response = requests.get(
            url,
            headers=headers,
            params=params,
            proxies=proxies,
            timeout=self.timeout
        )
        
        if response.status_code != 200:
            self.logger.error(f"Request failed with status {response.status_code}")
            response.raise_for_status()
        
        return response
    
    def _fetch_with_selenium(self, url: str) -> str:
        """Fetch page content using Selenium"""
        if not self.driver:
            self._setup_selenium()
        
        self.logger.info(f"Fetching with Selenium: {url}")
        self.driver.get(url)
        # Allow time for JavaScript to execute
        time.sleep(3)
        return self.driver.page_source
    
    def _parse_html(self, html_content: str) -> BeautifulSoup:
        """Parse HTML content with BeautifulSoup"""
        return BeautifulSoup(html_content, "html.parser")
    
    def close(self):
        """Close resources"""
        if self.driver:
            self.driver.quit()
    
    @abstractmethod
    def scrape(self, *args, **kwargs) -> Any:
        """Main scraping method to be implemented by subclasses"""
        pass