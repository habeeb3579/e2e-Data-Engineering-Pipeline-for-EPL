import logging
from typing import Any, List, Dict

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

class PostgresStorage:
    """Storage adapter for PostgreSQL."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str, table: str):
        """Initialize PostgreSQL storage.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            table: Table name
        """
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self.table = table
        self.conn = None
        self.initialized = False
        
    def _get_connection(self):
        """Get database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self.connection_params)
        return self.conn
        
    def _initialize_tables(self):
        """Initialize database tables if they don't exist."""
        if self.initialized:
            return
            
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS football;
            """)
            
            if self.table == "matches":
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS football.matches (
                        id VARCHAR PRIMARY KEY,
                        home_team VARCHAR NOT NULL,
                        away_team VARCHAR NOT NULL,
                        home_goals INTEGER,
                        away_goals INTEGER,
                        date TIMESTAMP,
                        league VARCHAR,
                        season VARCHAR,
                        is_finished BOOLEAN,
                        home_xg FLOAT,
                        away_xg FLOAT,
                        match_data JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            elif self.table == "player_stats":
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS football.player_stats (
                        id SERIAL PRIMARY KEY,
                        match_id VARCHAR NOT NULL,
                        player_id VARCHAR NOT NULL,
                        team VARCHAR,
                        minutes INTEGER,
                        goals INTEGER,
                        assists INTEGER,
                        xg FLOAT,
                        xa FLOAT,
                        shots INTEGER,
                        key_passes INTEGER,
                        position VARCHAR,
                        player_name VARCHAR,
                        league VARCHAR,
                        season VARCHAR,
                        stats_data JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(match_id, player_id)
                    );
                """)
            elif self.table == "shots":
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS football.shots (
                        id VARCHAR PRIMARY KEY,
                        match_id VARCHAR NOT NULL,
                        minute INTEGER,
                        player VARCHAR,
                        player_id VARCHAR,
                        team VARCHAR,
                        x FLOAT,
                        y FLOAT,
                        xg FLOAT,
                        result VARCHAR,
                        situation VARCHAR,
                        season VARCHAR,
                        league VARCHAR,
                        shot_data JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            
            conn.commit()
            self.initialized = True
            logger.info(f"Initialized table football.{self.table}")
        
    def _dataframe_to_records(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Convert PySpark DataFrame to list of records.
        
        Args:
            df: PySpark DataFrame
            
        Returns:
            List of dictionaries
        """
        return [row.asDict() for row in df.collect()]
        
    def store_matches(self, df: DataFrame) -> None:
        """Store match data.
        
        Args:
            df: PySpark DataFrame with match data
        """
        self._initialize_tables()
        records = self._dataframe_to_records(df)
        
        if not records:
            return
            
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Prepare data for insertion/update
            columns = ["id", "home_team", "away_team", "home_goals", "away_goals", 
                      "date", "league", "season", "is_finished", "home_xg", "away_xg", "match_data"]
            
            values = [(
                r.get("id"),
                r.get("h", {}).get("title"),
                r.get("a", {}).get("title"),
                r.get("goals", {}).get("h"),
                r.get("goals", {}).get("a"),
                r.get("date"),
                r.get("league"),
                r.get("season"),
                r.get("isResult", False),
                r.get("xG", {}).get("h"),
                r.get("xG", {}).get("a"),
                r
            ) for r in records]
            
            # Use execute_values for efficient bulk insert/update
            execute_values(
                cursor,
                f"""
                INSERT INTO football.matches ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    home_goals = EXCLUDED.home_goals,
                    away_goals = EXCLUDED.away_goals,
                    is_finished = EXCLUDED.is_finished,
                    home_xg = EXCLUDED.home_xg,
                    away_xg = EXCLUDED.away_xg,
                    match_data = EXCLUDED.match_data,
                    updated_at = CURRENT_TIMESTAMP
                """,
                values,
                template=f"(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
            )
            
            conn.commit()
            logger.info(f"Stored {len(records)} match records")
            
    def store_player_stats(self, df: DataFrame) -> None:
        """Store player stats data.
        
        Args:
            df: PySpark DataFrame with player stats data
        """
        self._initialize_tables()
        records = self._dataframe_to_records(df)
        
        if not records:
            return
            
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Prepare data for insertion/update
            columns = ["match_id", "player_id", "team", "minutes", "goals", "assists", 
                      "xg", "xa", "shots", "key_passes", "position", "player_name", 
                      "league", "season", "stats_data"]
            
            values = [(
                r.get("match_id"),
                r.get("player_id"),
                r.get("team"),
                r.get("time"),
                r.get("goals"),
                r.get("assists"),
                r.get("xG"),
                r.get("xA"),
                r.get("shots"),
                r.get("key_passes"),
                r.get("position"),
                r.get("player"),
                r.get("league"),
                r.get("season"),
                r
            ) for r in records]
            
            # Use execute_values for efficient bulk insert/update
            execute_values(
                cursor,
                f"""
                INSERT INTO football.player_stats ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (match_id, player_id) DO UPDATE SET
                    minutes = EXCLUDED.minutes,
                    goals = EXCLUDED.goals,
                    assists = EXCLUDED.assists,
                    xg = EXCLUDED.xg,
                    xa = EXCLUDED.xa,
                    shots = EXCLUDED.shots,
                    key_passes = EXCLUDED.key_passes,
                    stats_data = EXCLUDED.stats_data
                """,
                values,
                template=f"(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
            )
            
            conn.commit()
            logger.info(f"Stored {len(records)} player stats records")
            
    def store_shots(self, df: DataFrame) -> None:
        """Store shot data.
        
        Args:
            df: PySpark DataFrame with shot data
        """
        self._initialize_tables()
        records = self._dataframe_to_records(df)
        
        if not records:
            return
            
        conn = self._get_connection()
        with conn.cursor() as cursor:
            # Prepare data for insertion/update
            columns = ["id", "match_id", "minute", "player", "player_id", "team", 
                      "x", "y", "xg", "result", "situation", "season", "league", "shot_data"]
            
            values = [(
                r.get("id", f"{r.get('match_id')}_{r.get('player_id')}_{r.get('minute')}"),
                r.get("match_id"),
                r.get("minute"),
                r.get("player"),
                r.get("player_id"),
                r.get("team"),
                r.get("X"),
                r.get("Y"),
                r.get("xG"),
                r.get("result"),
                r.get("situation"),
                r.get("season"),
                r.get("league"),
                r
            ) for r in records]
            
            # Use execute_values for efficient bulk insert/update
            execute_values(
                cursor,
                f"""
                INSERT INTO football.shots ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    result = EXCLUDED.result,
                    xg = EXCLUDED.xg,
                    shot_data = EXCLUDED.shot_data
                """,
                values,
                template=f"(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
            )
            
            conn.commit()
            logger.info(f"Stored {len(records)} shot records")