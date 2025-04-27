import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.dbt.operators.dbt import DbtRunOperator
from airflow.utils.dates import days_ago

# Import the pipeline modules
import sys
sys.path.append('/opt/airflow/src')
from src.extraction.understat.loader import UnderstatLoader
from src.utils.config import ConfigManager, StorageType

# Get pipeline configuration
config_path = os.environ.get('FOOTBALL_CONFIG_PATH', '/opt/airflow/config/config.yaml')
config_manager = ConfigManager(config_path)
config = config_manager.get_config()

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the main ETL DAG
with DAG(
    'football_etl_pipeline',
    default_args=default_args,
    description='End-to-end football data pipeline from understat.com',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['football', 'understat', 'etl'],
) as dag:
    
    # Task to extract league data
    extract_leagues = PythonOperator(
        task_id='extract_leagues',
        python_callable=lambda: UnderstatLoader(config).load_to_storage('leagues'),
        dag=dag,
    )
    
    # Task to extract team data
    extract_teams = PythonOperator(
        task_id='extract_teams',
        python_callable=lambda: UnderstatLoader(config).load_to_storage('teams'),
        dag=dag,
    )
    
    # Task to extract player data
    extract_players = PythonOperator(
        task_id='extract_players',
        python_callable=lambda: UnderstatLoader(config).load_to_storage('players'),
        dag=dag,
    )
    
    # Task to extract match data
    extract_matches = PythonOperator(
        task_id='extract_matches',
        python_callable=lambda: UnderstatLoader(config).load_to_storage('matches'),
        dag=dag,
    )
    
    # Define PySpark processing tasks
    pyspark_base_args = {
        'conn_id': 'spark_default',
        'application': '{{ dag_run.conf.get("spark_application", "/opt/airflow/src/processing/run_processor.py") }}',
        'driver_memory': '2g',
        'executor_memory': '4g',
        'num_executors': 2,
        'executor_cores': 2,
        'conf': {'spark.driver.extraJavaOptions': '-Dlog4j.logLevel=info'},
    }
    
    # Process league data
    process_leagues = SparkSubmitOperator(
        task_id='process_leagues',
        application_args=['--processor_type', 'league', '--config_path', config_path],
        **pyspark_base_args,
        dag=dag,
    )
    
    # Process team data
    process_teams = SparkSubmitOperator(
        task_id='process_teams',
        application_args=['--processor_type', 'team', '--config_path', config_path],
        **pyspark_base_args,
        dag=dag,
    )
    
    # Process player data
    process_players = SparkSubmitOperator(
        task_id='process_players',
        application_args=['--processor_type', 'player', '--config_path', config_path],
        **pyspark_base_args,
        dag=dag,
    )
    
    # Process match data
    process_matches = SparkSubmitOperator(
        task_id='process_matches',
        application_args=['--processor_type', 'match', '--config_path', config_path],
        **pyspark_base_args,
        dag=dag,
    )
    
    # Conditional task for loading to the proper storage system
    if config.storage_type == StorageType.POSTGRES:
        # Run dbt models on PostgreSQL
        run_dbt_models = DbtRunOperator(
            task_id='run_dbt_models',
            dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt/profiles',
            target='postgres',
            dag=dag,
        )
        
        # Create view for dashboard
        create_dashboard_view = PostgresOperator(
            task_id='create_dashboard_view',
            postgres_conn_id='postgres_default',
            sql='sql/create_dashboard_views.sql',
            dag=dag,
        )
        
        # Define task dependencies for Postgres path
        extract_leagues >> process_leagues
        extract_teams >> process_teams
        extract_players >> process_players
        extract_matches >> process_matches
        [process_leagues, process_teams, process_players, process_matches] >> run_dbt_models >> create_dashboard_view
        
    else:  # GCP BigQuery path
        # Run dbt models on BigQuery
        run_dbt_models = DbtRunOperator(
            task_id='run_dbt_models',
            dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt/profiles',
            target='bigquery',
            dag=dag,
        )
        
        # Create view for dashboard in BigQuery
        create_dashboard_view = BigQueryOperator(
            task_id='create_dashboard_view',
            use_legacy_sql=False,
            sql_path_prefix='/opt/airflow/dbt/models/dashboard/',
            sql='SELECT * FROM dashboard.combined_stats',
            destination_dataset_table='{{ var.value.bigquery_dataset }}.dashboard_view',
            write_disposition='WRITE_TRUNCATE',
            dag=dag,
        )
        
        # Define task dependencies for GCP path
        extract_leagues >> process_leagues
        extract_teams >> process_teams
        extract_players >> process_players
        extract_matches >> process_matches
        [process_leagues, process_teams, process_players, process_matches] >> run_dbt_models >> create_dashboard_view

# Add a separate DAG for real-time updates using streaming
with DAG(
    'football_streaming_pipeline',
    default_args=default_args,
    description='Stream processing for new football match data',
    schedule_interval=timedelta(hours=6),  # Check for updates every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['football', 'understat', 'streaming'],
) as streaming_dag:
    
    # Sensor to detect new match data
    check_new_matches = PythonOperator(
        task_id='check_new_matches',
        python_callable=lambda: UnderstatLoader(config).check_for_new_matches(),
        dag=streaming_dag,
    )
    
    # Stream new match data if available
    stream_new_matches = PythonOperator(
        task_id='stream_new_matches',
        python_callable=lambda: UnderstatLoader(config).stream_new_matches(),
        dag=streaming_dag,
    )
    
    # Process new match data with Flink
    process_streamed_matches = PythonOperator(
        task_id='process_streamed_matches',
        python_callable=lambda: None,  # Will be implemented in streaming module
        dag=streaming_dag,
    )
    
    # Define streaming task dependencies
    check_new_matches >> stream_new_matches >> process_streamed_matches