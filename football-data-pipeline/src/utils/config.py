import os
import enum
from typing import Dict, List, Optional, Any
import yaml
from dataclasses import dataclass


class StorageType(enum.Enum):
    """Storage types supported by the pipeline"""
    POSTGRES = "postgres"
    GCP = "gcp"


class ScraperMethod(enum.Enum):
    """Scraper methods supported by the pipeline"""
    BEAUTIFULSOUP = "beautifulsoup"
    SELENIUM = "selenium"


@dataclass
class ScraperConfig:
    """Configuration for the understat scraper"""
    leagues: List[str]
    seasons: List[str]
    current_season: str
    use_proxy: bool = False
    proxy_url: Optional[str] = None
    request_delay: float = 1.0
    scraper_method: str = "beautifulsoup"
    headless: bool = True


@dataclass
class PostgresConfig:
    """Configuration for PostgreSQL database"""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "football"


@dataclass
class GCPConfig:
    """Configuration for Google Cloud Platform"""
    project_id: str
    location: str
    bucket_name: str
    dataset_id: str
    service_account_key_path: Optional[str] = None


@dataclass
class SparkConfig:
    """Configuration for Apache Spark"""
    master_url: str
    app_name: str = "FootballDataPipeline"
    driver_memory: str = "2g"
    executor_memory: str = "4g"
    executor_cores: int = 2
    num_executors: int = 2


@dataclass
class StreamingConfig:
    """Configuration for streaming components"""
    kafka_bootstrap_servers: str
    kafka_topic: str
    schema_registry_url: Optional[str] = None
    consumer_group_id: str = "football-consumer"


@dataclass
class PipelineConfig:
    """Main configuration for the football data pipeline"""
    storage_type: StorageType
    scraper: ScraperConfig
    output_dir: str
    postgres: Optional[PostgresConfig] = None
    gcp: Optional[GCPConfig] = None
    spark: Optional[SparkConfig] = None
    streaming: Optional[StreamingConfig] = None
    dbt_profiles_dir: str = "./dbt/profiles"
    update_frequency: str = "daily"  # daily, hourly, weekly


class ConfigManager:
    """Manages the configuration for the pipeline"""
    
    def __init__(self, config_path: str):
        """Initialize with the path to the config file"""
        self.config_path = config_path
        self._config = None
    
    def _load_config_from_file(self) -> Dict:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_config(self) -> PipelineConfig:
        """Get the pipeline configuration"""
        if self._config is not None:
            return self._config
        
        config_dict = self._load_config_from_file()
        
        # Create scraper config
        scraper_config = ScraperConfig(
            leagues=config_dict.get("scraper", {}).get("leagues", ["epl"]),
            seasons=config_dict.get("scraper", {}).get("seasons", ["2023"]),
            current_season=config_dict.get("scraper", {}).get("current_season", "2023"),
            use_proxy=config_dict.get("scraper", {}).get("use_proxy", False),
            proxy_url=config_dict.get("scraper", {}).get("proxy_url"),
            request_delay=config_dict.get("scraper", {}).get("request_delay", 1.0),
            scraper_method=config_dict.get("scraper", {}).get("method", "beautifulsoup"),
            headless=config_dict.get("scraper", {}).get("headless", True)
        )
        
        # Determine storage type
        storage_type_str = config_dict.get("storage_type", "postgres").lower()
        storage_type = StorageType.POSTGRES if storage_type_str == "postgres" else StorageType.GCP
        
        # Create appropriate storage config
        postgres_config = None
        gcp_config = None
        
        if storage_type == StorageType.POSTGRES:
            postgres_dict = config_dict.get("postgres", {})
            postgres_config = PostgresConfig(
                host=postgres_dict.get("host", "localhost"),
                port=postgres_dict.get("port", 5432),
                database=postgres_dict.get("database", "football"),
                username=postgres_dict.get("username", "postgres"),
                password=postgres_dict.get("password", "password"),
                schema=postgres_dict.get("schema", "football")
            )
        else:
            gcp_dict = config_dict.get("gcp", {})
            gcp_config = GCPConfig(
                project_id=gcp_dict.get("project_id", ""),
                location=gcp_dict.get("location", "US"),
                bucket_name=gcp_dict.get("bucket_name", ""),
                dataset_id=gcp_dict.get("dataset_id", "football_data"),
                service_account_key_path=gcp_dict.get("service_account_key_path")
            )
        
        # Create Spark config
        spark_dict = config_dict.get("spark", {})
        spark_config = SparkConfig(
            master_url=spark_dict.get("master_url", "local[*]"),
            app_name=spark_dict.get("app_name", "FootballDataPipeline"),
            driver_memory=spark_dict.get("driver_memory", "2g"),
            executor_memory=spark_dict.get("executor_memory", "4g"),
            executor_cores=spark_dict.get("executor_cores", 2),
            num_executors=spark_dict.get("num_executors", 2)
        )
        
        # Create streaming config
        streaming_dict = config_dict.get("streaming", {})
        streaming_config = StreamingConfig(
            kafka_bootstrap_servers=streaming_dict.get("kafka_bootstrap_servers", "localhost:9092"),
            kafka_topic=streaming_dict.get("kafka_topic", "football-data"),
            schema_registry_url=streaming_dict.get("schema_registry_url"),
            consumer_group_id=streaming_dict.get("consumer_group_id", "football-consumer")
        )
        
        # Create main pipeline config
        self._config = PipelineConfig(
            storage_type=storage_type,
            scraper=scraper_config,
            output_dir=config_dict.get("output_dir", "./data"),
            postgres=postgres_config,
            gcp=gcp_config,
            spark=spark_config,
            streaming=streaming_config,
            dbt_profiles_dir=config_dict.get("dbt_profiles_dir", "./dbt/profiles"),
            update_frequency=config_dict.get("update_frequency", "daily")
        )
        
        return self._config