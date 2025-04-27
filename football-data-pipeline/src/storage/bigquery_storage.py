import logging
import os
from typing import Any, Dict, List

from google.cloud import bigquery
from google.oauth2 import service_account
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

class BigQueryStorage:
    """Storage adapter for Google BigQuery."""
    
    def __init__(self, project_id: str, dataset: str, table: str, credentials_path: str = None):
        """Initialize BigQuery storage.
        
        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: Table name
            credentials_path: Path to service account credentials file
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.credentials_path = credentials_path
        self.client = self._get_client()
        self.initialized = False
        
    def _get_client(self) -> bigquery.Client:
        """Get BigQuery client.
        
        Returns:
            BigQuery client
        """
        if self.credentials_path and os.path.exists(self.credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            return bigquery.Client(credentials=credentials, project=self.project_id)
        else:
            return bigquery.Client(project=self.project_id)
            
    def _initialize_tables(self):
        """Initialize BigQuery tables if they don't exist."""
        if self.initialized:
            return
            
        # Check if the dataset exists, create if not
        dataset_ref = self.client.dataset(self.dataset)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            logger.info(f"Created dataset {self.dataset}")
        
        # Check if the table exists, create if not
        table_ref = dataset_ref.table(self.table)
        try:
            self.client.get_table(table_ref)
        except Exception:
            # Define table schema based on table name
            if self.table == "matches":
                schema = [
                    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("home_team", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("away_team", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("home_goals", "INTEGER"),
                    bigquery.SchemaField("away_goals", "INTEGER"),
                    bigquery.SchemaField("date", "TIMESTAMP"),
                    bigquery.SchemaField("league", "STRING"),
                    bigquery.SchemaField("season", "STRING"),
                    bigquery.SchemaField("is_finished", "BOOLEAN"),
                    bigquery.SchemaField("home_xg", "FLOAT"),
                    bigquery.SchemaField("away_xg", "FLOAT"),
                    bigquery.SchemaField("match_data", "JSON"),
                    bigquery.SchemaField("created_at", "TIMESTAMP"),
                    bigquery.SchemaField("updated_at", "TIMESTAMP")
                ]
            elif self.table == "player_stats":
                schema = [
                    bigquery.SchemaField("match_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("player_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("team", "STRING"),
                    bigquery.SchemaField("minutes", "INTEGER"),
                    bigquery.SchemaField("goals", "INTEGER"),
                    bigquery.SchemaField("assists", "INTEGER"),
                    bigquery.SchemaField("xg", "FLOAT"),
                    bigquery.SchemaField("xa", "FLOAT"),
                    bigquery.SchemaField("shots", "INTEGER"),
                    bigquery.SchemaField("key_passes", "INTEGER"),
                    bigquery.SchemaField("position", "STRING"),
                    bigquery.SchemaField("player_name", "STRING"),
                    bigquery.SchemaField("league", "STRING"),
                    bigquery.SchemaField("season", "STRING"),
                    bigquery.SchemaField("stats_data", "JSON"),
                    bigquery.SchemaField("created_at", "TIMESTAMP")
                ]
            elif self.table == "shots":
                schema = [
                    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("match_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("minute", "INTEGER"),
                    bigquery.SchemaField("player", "STRING"),
                    bigquery.SchemaField("player_id", "STRING"),
                    bigquery.SchemaField("team", "STRING"),
                    bigquery.SchemaField("x", "FLOAT"),
                    bigquery.SchemaField("y", "FLOAT"),
                    bigquery.SchemaField("xg", "FLOAT"),
                    bigquery.SchemaField("result", "STRING"),
                    bigquery.SchemaField("situation", "STRING"),
                    bigquery.SchemaField("season", "STRING"),
                    bigquery.SchemaField("league", "STRING"),
                    bigquery.SchemaField("shot_data", "JSON"),
                    bigquery.SchemaField("created_at", "TIMESTAMP")
                ]
            else:
                # Generic schema for unknown tables
                schema = [
                    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("data", "JSON"),
                    bigquery.SchemaField("created_at", "TIMESTAMP")
                ]
                
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logger.info(f"Created table {self.dataset}.{self.table}")
            
        self.initialized = True
            
    def _dataframe_to_records(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Convert PySpark DataFrame to list of records.
        
        Args:
            df: PySpark DataFrame
            
        Returns:
            List of dictionaries
        """
        return [row.asDict() for row in df.collect()]
    
    def store_matches(self, df: DataFrame) -> None:
        """Store match data in BigQuery.
        
        Args:
            df: PySpark DataFrame with match data
        """
        self._initialize_tables()
        records = self._dataframe_to_records(df)
        
        if not records:
            return
            
        # Transform records to BigQuery format
        rows = []
        for r in records:
            row = {
                "id": r.get("id"),
                "home_team": r.get("h", {}).get("title"),
                "away_team": r.get("a", {}).get("title"),
                "home_goals": r.get("goals", {}).get("h"),
                "away_goals": r.get("goals", {}).get("a"),
                "date": r.get("date"),
                "league": r.get("league"),
                "season": r.get("season"),
                "is_finished": r.get("isResult", False),
                "home_xg": r.get("xG", {}).get("h"),
                "away_xg": r.get("xG", {}).get("a"),
                "match_data": r,
                "created_at": bigquery.ScalarQueryParameter("", "TIMESTAMP", "CURRENT_TIMESTAMP").value,
                "updated_at": bigquery.ScalarQueryParameter("", "TIMESTAMP", "CURRENT_TIMESTAMP").value
            }
            rows.append(row)
            
        # Insert rows using the streaming API
        table_ref = self.client.dataset(self.dataset).table(self.table)
        errors = self.client.insert_rows_json(table_ref, rows)
        
        if errors:
            logger.error(f"Errors inserting match rows: {errors}")
        else:
            logger.info(f"Stored {len(rows)} match records in BigQuery")
            
    def store_player_stats(self, df: DataFrame) -> None:
        """Store player stats data in BigQuery.
        
        Args:
            df: PySpark DataFrame with player stats data
        """
        self._initialize_tables()
        records = self._dataframe_to_records(df)
        
        if not records:
            return
            
        # Transform records to BigQuery format
        rows = []
        for r in records:
            row = {
                "match_id": r.get("match_id"),
                "player_id": r.get("player_id"),
                "team": r.get("team"),
                "minutes": r.get("time"),
                "goals": r.get("goals"),
                "assists": r.get("assists"),
                "xg": r.get("xG"),
                "xa": r.get("xA"),
                "shots": r.get("shots"),
                "key_passes": r.get("key_passes"),
                "position": r.get("position"),
                "player_name": r.get("player"),
                "league": r.get("league"),
                "season": r.get("season"),
                "stats_data": r,
                "created_at": bigquery.ScalarQueryParameter("", "TIMESTAMP", "CURRENT_TIMESTAMP").value
            }
            rows.append(row)
            
        # Insert rows using the streaming API
        table_ref = self.client.dataset(self.dataset).table(self.table)
        errors = self.client.insert_rows_json(table_ref, rows)
        
        if errors:
            logger.error(f"Errors inserting player stats rows: {errors}")
        else:
            logger.info(f"Stored {len(rows)} player stats records in BigQuery")
            
    def store_shots(self, df: DataFrame) -> None:
        """Store shot data in BigQuery.
        
        Args:
            df: PySpark DataFrame with shot data
        """
        self._initialize_tables()
        records = self._dataframe_to_records(df)
        
        if not records:
            return
            
        # Transform records to BigQuery format
        rows = []
        for r in records:
            shot_id = r.get("id", f"{r.get('match_id')}_{r.get('player_id')}_{r.get('minute')}")
            row = {
                "id": shot_id,
                "match_id": r.get("match_id"),
                "minute": r.get("minute"),
                "player": r.get("player"),
                "player_id": r.get("player_id"),
                "team": r.get("team"),
                "x": r.get("X"),
                "y": r.get("Y"),
                "xg": r.get("xG"),
                "result": r.get("result"),
                "situation": r.get("situation"),
                "season": r.get("season"),
                "league": r.get("league"),
                "shot_data": r,
                "created_at": bigquery.ScalarQueryParameter("", "TIMESTAMP", "CURRENT_TIMESTAMP").value
            }
            rows.append(row)
            
        # Insert rows using the streaming API
        table_ref = self.client.dataset(self.dataset).table(self.table)
        errors = self.client.insert_rows_json(table_ref, rows)
        
        if errors:
            logger.error(f"Errors inserting shot rows: {errors}")
        else:
            logger.info(f"Stored {len(rows)} shot records in BigQuery")