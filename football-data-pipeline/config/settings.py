# config/settings.py

import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass

PROJECT_ROOT = Path(__file__).parent.parent


@dataclass
class ScraperConfig:
    base_url: str = "https://understat.com"
    request_timeout: int = 30
    use_proxies: bool = False
    rate_limit: int = 5  # requests per minute
    user_agent_rotation: bool = True
    headers: Dict[str, str] = None
    

@dataclass
class StreamingConfig:
    broker_urls: List[str] = None
    topic_prefix: str = "football_data"
    schema_registry_url: str = None
    auto_create_topics: bool = True
    batch_size: int = 100
    compression_type: str = "gzip"


@dataclass
class ProcessingConfig:
    spark_master: str = "local[*]"
    checkpoint_location: str = None
    window_duration: str = "1 minute"
    watermark_delay: str = "10 minutes"
    batch_interval: str = "5 minutes"


@dataclass
class StorageConfig:
    storage_type: str = "postgres"  # 'postgres' or 'gcp'
    postgres_config_path: Path = None
    gcp_config_path: Path = None
    

class Config:
    def __init__(self):
        self.env = os.getenv("ENVIRONMENT", "development")
        
        # Default settings
        self.scraper = ScraperConfig(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )
        
        self.streaming = StreamingConfig(
            broker_urls=["localhost:9092"],
            schema_registry_url="http://localhost:8081"
        )
        
        self.processing = ProcessingConfig(
            checkpoint_location=str(PROJECT_ROOT / "data" / "checkpoints"),
        )
        
        self.storage = StorageConfig(
            postgres_config_path=PROJECT_ROOT / "config" / "postgres_config.yaml",
            gcp_config_path=PROJECT_ROOT / "config" / "gcp_config.yaml",
        )
        
        # Leagues and seasons to scrape
        self.leagues = [
            "EPL",       # English Premier League
            "La_liga",   # Spanish La Liga
            "Bundesliga", # German Bundesliga
            "Serie_A",   # Italian Serie A
            "Ligue_1",   # French Ligue 1
        ]
        
        # Number of past seasons to scrape
        self.seasons = [str(year) for year in range(2015, 2026)]
        
        # Logging configuration
        self.log_level = "INFO"
        self.log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        self.log_dir = PROJECT_ROOT / "logs"
        
        # Load environment-specific configuration
        self._load_env_config()
    
    def _load_env_config(self):
        """Load environment-specific configuration from YAML files"""
        env_config_path = PROJECT_ROOT / "config" / f"{self.env}.yaml"
        if env_config_path.exists():
            with open(env_config_path, "r") as f:
                env_config = yaml.safe_load(f)
                self._update_config(env_config)
    
    def _update_config(self, config_dict: Dict[str, Any]):
        """Update configuration from dictionary"""
        for section, values in config_dict.items():
            if hasattr(self, section):
                section_obj = getattr(self, section)
                for key, value in values.items():
                    if hasattr(section_obj, key):
                        setattr(section_obj, key, value)
    
    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration based on storage type"""
        if self.storage.storage_type == "postgres":
            with open(self.storage.postgres_config_path, "r") as f:
                return yaml.safe_load(f)
        elif self.storage.storage_type == "gcp":
            with open(self.storage.gcp_config_path, "r") as f:
                return yaml.safe_load(f)
        else:
            raise ValueError(f"Unsupported storage type: {self.storage.storage_type}")


# Create global configuration instance
config = Config()