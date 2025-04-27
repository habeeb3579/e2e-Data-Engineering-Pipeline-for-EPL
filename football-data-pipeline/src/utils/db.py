#!/usr/bin/env python3
"""
Database utility functions for connecting to different data sources.
"""

import logging
import psycopg2
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_postgres_connection(config):
    """
    Get a PostgreSQL database connection.
    
    Args:
        config: Configuration object with database settings
        
    Returns:
        psycopg2.connection: PostgreSQL connection object
    """
    try:
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        logger.info(f"Successfully connected to PostgreSQL database: {config.postgres.database}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise

def get_bigquery_client(config):
    """
    Get a BigQuery client.
    
    Args:
        config: Configuration object with GCP settings
        
    Returns:
        bigquery.Client: BigQuery client object
    """
    try:
        if config.gcp.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                config.gcp.credentials_path
            )
            client = bigquery.Client(
                project=config.gcp.project_id,
                credentials=credentials
            )
        else:
            client = bigquery.Client(project=config.gcp.project_id)
            
        logger.info(f"Successfully connected to BigQuery: {config.gcp.project_id}")
        return client
    except Exception as e:
        logger.error(f"Error connecting to BigQuery: {e}")
        raise

def execute_query(conn, query: str, params: Optional[Dict[str, Any]] = None):
    """
    Execute a SQL query on a database connection.
    
    Args:
        conn: Database connection object (psycopg2 connection or bigquery client)
        query: SQL query string
        params: Optional parameters for the query
        
    Returns:
        pandas.DataFrame: Query results as a DataFrame
    """
    try:
        if isinstance(conn, psycopg2.extensions.connection):
            # PostgreSQL connection
            return pd.read_sql_query(query, conn, params=params)
        elif isinstance(conn, bigquery.Client):
            # BigQuery client
            job_config = bigquery.QueryJobConfig(
                query_parameters=params if params else []
            )
            query_job = conn.query(query, job_config=job_config)
            return query_job.to_dataframe()
        else:
            raise ValueError(f"Unsupported connection type: {type(conn)}")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise

def insert_dataframe(conn, df: pd.DataFrame, table_name: str, schema: str = "raw"):
    """
    Insert a DataFrame into a database table.
    
    Args:
        conn: Database connection object (psycopg2 connection or bigquery client)
        df: DataFrame to insert
        table_name: Name of the target table
        schema: Database schema name (default: "raw")
    """
    try:
        if isinstance(conn, psycopg2.extensions.connection):
            # PostgreSQL connection
            full_table_name = f"{schema}.{table_name}"
            
            # Use pandas to_sql to insert data
            df.to_sql(
                table_name,
                conn,
                schema=schema,
                if_exists="append",
                index=False
            )
            logger.info(f"Successfully inserted {len(df)} rows into {full_table_name}")
            
        elif isinstance(conn, bigquery.Client):
            # BigQuery client
            full_table_name = f"{conn.project}.{schema}.{table_name}"
            
            # Use the BigQuery load_table_from_dataframe method
            job_config = bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField(name, get_bq_type(df[name].dtype))
                        for name in df.columns],
                write_disposition="WRITE_APPEND",
            )
            
            load_job = conn.load_table_from_dataframe(
                df, full_table_name, job_config=job_config
            )
            load_job.result()  # Wait for the job to complete
            
            logger.info(f"Successfully inserted {len(df)} rows into {full_table_name}")
        else:
            raise ValueError(f"Unsupported connection type: {type(conn)}")
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        raise

def get_bq_type(dtype):
    """
    Map pandas dtype to BigQuery data type.
    
    Args:
        dtype: pandas dtype
        
    Returns:
        str: BigQuery data type
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "STRING"