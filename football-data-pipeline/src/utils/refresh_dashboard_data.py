#!/usr/bin/env python3
"""
Utility script to refresh dashboard data.
This script is called by the Kestra workflow after dbt transformations.
"""

import os
import yaml
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

from src.utils.config import Config
from src.utils.db import get_postgres_connection, get_bigquery_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def refresh_postgres_data(config):
    """Refresh data from PostgreSQL for the dashboard"""
    logger.info("Refreshing dashboard data from PostgreSQL")
    
    try:
        # Connect to PostgreSQL
        conn = get_postgres_connection(config)
        
        # Define queries to extract data
        queries = {
            "team_stats": "SELECT * FROM analytics.team_stats",
            "player_stats": "SELECT * FROM analytics.player_stats",
            "match_stats": "SELECT * FROM analytics.match_stats",
            "league_standings": "SELECT * FROM analytics.league_standings"
        }
        
        # Create data directory if it doesn't exist
        data_dir = Path("/app/data")
        data_dir.mkdir(exist_ok=True)
        
        # Execute queries and save results
        for name, query in queries.items():
            logger.info(f"Executing query: {name}")
            df = pd.read_sql_query(query, conn)
            df.to_parquet(data_dir / f"{name}.parquet")
            
        conn.close()
        logger.info("Successfully refreshed dashboard data")
        
    except Exception as e:
        logger.error(f"Error refreshing dashboard data: {e}")
        raise

def refresh_bigquery_data(config):
    """Refresh data from BigQuery for the dashboard"""
    logger.info("Refreshing dashboard data from BigQuery")
    
    try:
        # Connect to BigQuery
        client = get_bigquery_client(config)
        
        # Define queries to extract data
        queries = {
            "team_stats": "SELECT * FROM `{}.analytics.team_stats`".format(config.gcp.project_id),
            "player_stats": "SELECT * FROM `{}.analytics.player_stats`".format(config.gcp.project_id),
            "match_stats": "SELECT * FROM `{}.analytics.match_stats`".format(config.gcp.project_id),
            "league_standings": "SELECT * FROM `{}.analytics.league_standings`".format(config.gcp.project_id)
        }
        
        # Create data directory if it doesn't exist
        data_dir = Path("/app/data")
        data_dir.mkdir(exist_ok=True)
        
        # Execute queries and save results
        for name, query in queries.items():
            logger.info(f"Executing query: {name}")
            df = client.query(query).to_dataframe()
            df.to_parquet(data_dir / f"{name}.parquet")
            
        logger.info("Successfully refreshed dashboard data")
        
    except Exception as e:
        logger.error(f"Error refreshing dashboard data: {e}")
        raise

def main():
    """Main function to refresh dashboard data based on configuration"""
    logger.info("Starting dashboard data refresh")
    
    # Load configuration
    config = Config()
    
    # Refresh data based on storage type
    if config.storage.type.lower() == "postgres":
        refresh_postgres_data(config)
    elif config.storage.type.lower() == "bigquery":
        refresh_bigquery_data(config)
    else:
        logger.error(f"Unsupported storage type: {config.storage.type}")
        
    logger.info("Dashboard data refresh completed")

if __name__ == "__main__":
    main()