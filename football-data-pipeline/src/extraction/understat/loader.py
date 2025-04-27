import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor
import dlt
from dlt.destinations import bigquery, postgres

from src.extraction.understat.scraper import UnderstatScraperFactory
from src.utils.config import PipelineConfig, StorageType

logger = logging.getLogger(__name__)

class UnderstatLoader:
    """Loads data from understat.com to the configured storage"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.output_dir = Path(config.output_dir)
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _create_dlt_pipeline(self, name: str) -> dlt.Pipeline:
        """Create a dlt pipeline based on the storage configuration"""
        if self.config.storage_type == StorageType.POSTGRES:
            return dlt.pipeline(
                pipeline_name=f"understat_{name}",
                destination=postgres(
                    connection_string=f"postgresql://{self.config.postgres.username}:{self.config.postgres.password}@"
                                      f"{self.config.postgres.host}:{self.config.postgres.port}/{self.config.postgres.database}",
                    schema=self.config.postgres.schema
                ),
                dataset_name=f"understat_{name}"
            )
        else:  # GCP
            return dlt.pipeline(
                pipeline_name=f"understat_{name}",
                destination=bigquery(
                    project_id=self.config.gcp.project_id,
                    location=self.config.gcp.location,
                    dataset_name=self.config.gcp.dataset_id,
                    credentials=self.config.gcp.service_account_key_path
                ),
                dataset_name=f"understat_{name}"
            )
    
    def _save_to_parquet(self, data: List[Dict], name: str) -> str:
        """Save data to parquet file in the output directory"""
        # Create a directory for the data type if it doesn't exist
        data_dir = self.output_dir / name
        os.makedirs(data_dir, exist_ok=True)
        
        # Create a filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.parquet"
        output_path = data_dir / filename
        
        # Convert to pandas DataFrame and save as parquet
        df = pd.DataFrame(data)
        df.to_parquet(output_path)
        
        logger.info(f"Saved {len(data)} {name} to {output_path}")
        return str(output_path)
    
    def _upload_to_gcs(self, local_path: str, blob_name: str) -> str:
        """Upload a file to Google Cloud Storage"""
        if self.config.storage_type != StorageType.GCP:
            return local_path
            
        # Initialize GCS client
        if self.config.gcp.service_account_key_path:
            client = storage.Client.from_service_account_json(self.config.gcp.service_account_key_path)
        else:
            client = storage.Client()
            
        bucket = client.bucket(self.config.gcp.bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        
        gcs_path = f"gs://{self.config.gcp.bucket_name}/{blob_name}"
        logger.info(f"Uploaded {local_path} to {gcs_path}")
        return gcs_path
    
    def _send_to_kafka(self, data: Dict, topic: str = None) -> None:
        """Send data to Kafka/Redpanda for streaming processing"""
        if not self.config.streaming:
            return
            
        # Use the specified topic or default from config
        if topic is None:
            topic = self.config.streaming.kafka_topic
            
        # Configure Kafka producer
        producer_config = {
            'bootstrap.servers': self.config.streaming.kafka_bootstrap_servers,
            'client.id': 'understat-producer'
        }
        
        # Create producer
        producer = Producer(producer_config)
        
        # Serialize data to JSON
        json_data = json.dumps(data).encode('utf-8')
        
        # Send to Kafka
        producer.produce(topic, json_data)
        producer.flush()
        
        logger.info(f"Sent data to Kafka topic {topic}")
    
    def check_for_new_matches(self) -> List[str]:
        """Check for new matches"""
        # Create a match scraper
        match_scraper = UnderstatScraperFactory.create_scraper("match", self.config.scraper)
        
        # Get new matches
        newest_processed_date = self._get_newest_processed_date()
        new_match_ids = match_scraper.check_for_new_matches(since_date=newest_processed_date)
        
        logger.info(f"Found {len(new_match_ids)} new matches since {newest_processed_date}")
        return new_match_ids
    
    def _get_newest_processed_date(self) -> str:
        """Get the date of the newest processed match"""
        # This would typically query the database or check file timestamps
        # For simplicity, we'll return a date 7 days ago
        from datetime import datetime, timedelta
        return (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    def stream_new_matches(self) -> None:
        """Stream new matches to Kafka/Redpanda"""
        # Get new match IDs
        new_match_ids = self.check_for_new_matches()
        
        if not new_match_ids:
            logger.info("No new matches to stream")
            return
            
        # Create a match scraper
        match_scraper = UnderstatScraperFactory.create_scraper("match", self.config.scraper)
        
        # Process each match and send to Kafka
        for match_id in new_match_ids:
            match_data = match_scraper.scrape_match(match_id)
            self._send_to_kafka(match_data, f"{self.config.streaming.kafka_topic}-matches")
    
    def _process_with_dlt(self, data: List[Dict], name: str) -> None:
        """Process data with dlt framework"""
        pipeline = self._create_dlt_pipeline(name)
        pipeline.run(data, table_name=name)
        logger.info(f"Loaded {len(data)} {name} with dlt")
    
    def load_to_storage(self, data_type: str) -> None:
        """Load data from understat.com to the configured storage"""
        # Create appropriate scraper
        scraper = UnderstatScraperFactory.create_scraper(data_type, self.config.scraper)
        
        # Scrape data
        logger.info(f"Scraping {data_type} data from understat.com")
        data = scraper.scrape()
        
        # Use dlt for loading data if we're using GCP or Postgres
        self._process_with_dlt(data, data_type)
        
        # Also save locally as parquet for backup/debugging
        local_path = self._save_to_parquet(data, data_type)
        
        # If using GCP, also upload to Cloud Storage
        if self.config.storage_type == StorageType.GCP:
            blob_name = f"{data_type}/{os.path.basename(local_path)}"
            self._upload_to_gcs(local_path, blob_name)
        
        # If we're scraping matches, also send to Kafka for real-time processing
        if data_type == "matches":
            for match in data:
                self._send_to_kafka(match, f"{self.config.streaming.kafka_topic}-matches")
    
    def load_all_to_storage(self, parallel: bool = True) -> None:
        """Load all data types to storage"""
        data_types = ["leagues", "teams", "players", "matches"]
        
        if parallel:
            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=len(data_types)) as executor:
                executor.map(self.load_to_storage, data_types)
        else:
            # Sequential processing
            for data_type in data_types:
                self.load_to_storage(data_type)