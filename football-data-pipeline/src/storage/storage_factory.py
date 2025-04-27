from typing import Dict, Any

from src.utils.config import Config
from src.storage.postgres_storage import PostgresStorage
from src.storage.bigquery_storage import BigQueryStorage


class StorageFactory:
    """Factory for creating storage instances."""
    
    def __init__(self, config: Config, storage_type: str = "postgres"):
        """Initialize the storage factory.
        
        Args:
            config: Config object
            storage_type: Storage type ("postgres" or "gcp")
        """
        self.config = config
        self.storage_type = storage_type
        self.topic_to_table_map = {
            "football-matches": "matches",
            "football-player-stats": "player_stats", 
            "football-shots": "shots"
        }
        self.storage_cache: Dict[str, Any] = {}
        
    def get_storage_for_topic(self, topic: str) -> Any:
        """Get storage instance for a topic.
        
        Args:
            topic: Topic name
            
        Returns:
            Storage instance
        """
        table = self.topic_to_table_map.get(topic, topic.replace("-", "_"))
        
        # Check if we already have a storage instance for this table
        if table in self.storage_cache:
            return self.storage_cache[table]
            
        # Create new storage instance
        if self.storage_type == "postgres":
            storage = PostgresStorage(
                host=self.config.get("postgres.host", "localhost"),
                port=self.config.get("postgres.port", 5432),
                database=self.config.get("postgres.database", "football"),
                user=self.config.get("postgres.user", "postgres"),
                password=self.config.get("postgres.password", ""),
                table=table
            )
        elif self.storage_type == "gcp":
            storage = BigQueryStorage(
                project_id=self.config.get("gcp.project_id"),
                dataset=self.config.get("gcp.dataset", "football"),
                table=table,
                credentials_path=self.config.get("gcp.credentials_path")
            )
        else:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
            
        # Cache the storage instance
        self.storage_cache[table] = storage
        return storage