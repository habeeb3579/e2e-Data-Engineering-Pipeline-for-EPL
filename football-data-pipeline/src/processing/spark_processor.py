# src/processing/spark_processor.py
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType

from src.utils.config import PipelineConfig, StorageType, SparkConfig

logger = logging.getLogger(__name__)

class BaseSparkProcessor(ABC):
    """Base class for processing football data with Spark"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session with the appropriate configuration"""
        assert self.config.spark is not None, "Spark configuration is required"
        
        # Build the Spark session
        builder = (
            SparkSession.builder
            .appName(self.config.spark.app_name)
            .master(self.config.spark.master)
            .config("spark.executor.memory", self.config.spark.executor_memory)
            .config("spark.driver.memory", self.config.spark.driver_memory)
            .config("spark.sql.session.timeZone", "UTC")
        )
        
        # Add GCP-specific configuration if using GCP
        if self.config.storage_type == StorageType.GCP and self.config.gcp is not None:
            builder = (
                builder
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            )
        
        return builder.getOrCreate()
    
    def _read_data(self, data_type: str) -> DataFrame:
        """Read data from the appropriate source"""
        if self.config.storage_type == StorageType.POSTGRES:
            assert self.config.postgres is not None, "PostgreSQL configuration is required"
            
            # Read from PostgreSQL
            jdbc_url = f"jdbc:postgresql://{self.config.postgres.host}:{self.config.postgres.port}/{self.config.postgres.database}"
            table_name = f"{self.config.postgres.schema}.{data_type}"
            
            return (
                self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", self.config.postgres.user)
                .option("password", self.config.postgres.password)
                .option("driver", "org.postgresql.Driver")
                .load()
            )
        else:  # GCP
            assert self.config.gcp is not None, "GCP configuration is required"
            
            # Read from BigQuery
            table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.{data_type}"
            
            return (
                self.spark.read
                .format("bigquery")
                .option("table", table_id)
                .load()
            )
    
    def _write_data(self, df: DataFrame, data_type: str, mode: str = "overwrite") -> None:
        """Write data to the appropriate destination"""
        if self.config.storage_type == StorageType.POSTGRES:
            assert self.config.postgres is not None, "PostgreSQL configuration is required"
            
            # Write to PostgreSQL
            jdbc_url = f"jdbc:postgresql://{self.config.postgres.host}:{self.config.postgres.port}/{self.config.postgres.database}"
            table_name = f"{self.config.postgres.schema}.{data_type}_processed"
            
            (
                df.write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", self.config.postgres.user)
                .option("password", self.config.postgres.password)
                .option("driver", "org.postgresql.Driver")
                .mode(mode)
                .save()
            )
        else:  # GCP
            assert self.config.gcp is not None, "GCP configuration is required"
            
            # Write to BigQuery
            table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.{data_type}_processed"
            
            (
                df.write
                .format("bigquery")
                .option("table", table_id)
                .mode(mode)
                .save()
            )
    
    @abstractmethod
    def process(self) -> None:
        """Process the data"""
        pass
    
    def close(self) -> None:
        """Close the Spark session"""
        if self.spark is not None:
            self.spark.stop()


class LeagueProcessor(BaseSparkProcessor):
    """Process league data"""
    
    def process(self) -> None:
        """Process league data from the raw source"""
        logger.info("Processing league data")
        
        # Read raw league data
        df = self._read_data("leagues")
        
        # Extract team data from the nested structure
        teams_df = df.select(
            F.col("league"),
            F.col("season"),
            F.explode(F.map_keys(F.col("teams"))).alias("team_id"),
            F.explode(F.map_values(F.col("teams"))).alias("team_data")
        )
        
        # Extract team statistics
        processed_df = teams_df.select(
            F.col("league"),
            F.col("season"),
            F.col("team_id"),
            F.col("team_data.title").alias("team_name"),
            F.col("team_data.history")
        ).withColumn(
            "matches", F.size(F.col("history"))
        ).withColumn(
            "latest_match", F.element_at(F.col("history"), -1)
        ).select(
            "league",
            "season",
            "team_id",
            "team_name",
            "matches",
            F.col("latest_match.wins").alias("wins"),
            F.col("latest_match.draws").alias("draws"),
            F.col("latest_match.loses").alias("losses"),
            F.col("latest_match.scored").alias("goals_scored"),
            F.col("latest_match.missed").alias("goals_conceded"),
            F.col("latest_match.pts").alias("points"),
            F.col("latest_match.xG").alias("expected_goals"),
            F.col("latest_match.xGA").alias("expected_goals_against"),
            F.col("latest_match.npxG").alias("non_penalty_expected_goals"),
            F.col("latest_match.deep").alias("deep_completions"),
            F.col("latest_match.ppda").alias("passes_per_defensive_action")
        )
        
        # Calculate additional metrics
        processed_df = processed_df.withColumn(
            "goal_difference", F.col("goals_scored") - F.col("goals_conceded")
        ).withColumn(
            "expected_goal_difference", F.col("expected_goals") - F.col("expected_goals_against")
        ).withColumn(
            "points_per_match", F.round(F.col("points") / F.col("matches"), 2)
        )
        
        # Write processed data
        self._write_data(processed_df, "league")
        logger.info("League data processing complete")


class MatchProcessor(BaseSparkProcessor):
    """Process match data"""
    
    def process(self) -> None:
        """Process match data from the raw source"""
        logger.info("Processing match data")
        
        # Read raw match data
        df = self._read_data("matches")
        
        # Process match data
        match_base_df = df.select(
            F.col("match_id"),
            F.col("league"),
            F.col("season"),
            F.col("date"),
            F.col("match_data")
        )
        
        # Extract and flatten match data
        match_processed_df = match_base_df.select(
            F.col("match_id"),
            F.col("league"),
            F.col("season"),
            F.to_timestamp(F.col("date"), "yyyy-MM-dd").alias("match_date"),
            F.col("match_data.h").alias("home_team_data"),
            F.col("match_data.a").alias("away_team_data"),
            F.col("match_data.goals.h").alias("home_goals"),
            F.col("match_data.goals.a").alias("away_goals"),
            F.col("match_data.xG.h").alias("home_xG"),
            F.col("match_data.xG.a").alias("away_xG"),
        ).select(
            "match_id",
            "league",
            "season",
            "match_date",
            F.col("home_team_data.id").alias("home_team_id"),
            F.col("home_team_data.title").alias("home_team_name"),
            F.col("away_team_data.id").alias("away_team_id"),
            F.col("away_team_data.title").alias("away_team_name"),
            "home_goals",
            "away_goals",
            "home_xG",
            "away_xG"
        )
        
        # Add calculated columns
        match_processed_df = match_processed_df.withColumn(
            "total_goals", F.col("home_goals") + F.col("away_goals")
        ).withColumn(
            "total_xG", F.round(F.col("home_xG") + F.col("away_xG"), 2)
        ).withColumn(
            "home_win", F.when(F.col("home_goals") > F.col("away_goals"), 1).otherwise(0)
        ).withColumn(
            "away_win", F.when(F.col("home_goals") < F.col("away_goals"), 1).otherwise(0)
        ).withColumn(
            "draw", F.when(F.col("home_goals") == F.col("away_goals"), 1).otherwise(0)
        ).withColumn(
            "season_year", F.year(F.col("match_date"))
        )
        
        # Extract shots data
        shots_df = df.select(
            F.col("match_id"),
            F.explode(F.col("shots_data.h")).alias("home_shots"),
            F.explode(F.col("shots_data.a")).alias("away_shots")
        )
        
        # Process home shots
        home_shots_df = shots_df.select(
            F.col("match_id"),
            F.col("home_shots.id").alias("shot_id"),
            F.col("home_shots.minute").alias("minute"),
            F.col("home_shots.player").alias("player"),
            F.col("home_shots.h_team").alias("team"),
            F.col("home_shots.a_team").alias("opponent"),
            F.col("home_shots.xG").alias("xG"),
            F.col("home_shots.result").alias("result"),
            F.lit("home").alias("team_type")
        )
        
        # Process away shots
        away_shots_df = shots_df.select(
            F.col("match_id"),
            F.col("away_shots.id").alias("shot_id"),
            F.col("away_shots.minute").alias("minute"),
            F.col("away_shots.player").alias("player"),
            F.col("away_shots.h_team").alias("opponent"),
            F.col("away_shots.a_team").alias("team"),
            F.col("away_shots.xG").alias("xG"),
            F.col("away_shots.result").alias("result"),
            F.lit("away").alias("team_type")
        )
        
        # Union home and away shots
        all_shots_df = home_shots_df.union(away_shots_df)
        
        # Write processed data
        self._write_data(match_processed_df, "matches")
        self._write_data(all_shots_df, "shots")
        logger.info("Match data processing complete")


class PlayerProcessor(BaseSparkProcessor):
    """Process player data"""
    
    def process(self) -> None:
        """Process player data from the raw source"""
        logger.info("Processing player data")
        
        # Read raw player data
        df = self._read_data("players")
        
        # Process player data
        player_df = df.select(
            F.col("player_id"),
            F.col("player_data")
        )
        
        # Extract player base info
        player_base_df = player_df.select(
            F.col("player_id"),
            F.col("player_data.id").alias("player_id_internal"),
            F.col("player_data.player_name").alias("name"),
            F.col("player_data.team_name").alias("current_team"),
            F.col("player_data.position").alias("position"),
            F.col("player_data.games").alias("total_games"),
            F.col("player_data.time").alias("total_minutes"),
            F.col("player_data.goals").alias("total_goals"),
            F.col("player_data.assists").alias("total_assists"),
            F.col("player_data.shots").alias("total_shots"),
            F.col("player_data.xG").alias("total_xG"),
            F.col("player_data.xA").alias("total_xA"),
            F.col("player_data.yellow_cards").alias("yellow_cards"),
            F.col("player_data.red_cards").alias("red_cards")
        )
        
        # Process player match data
        player_matches_df = df.select(
            F.col("player_id"),
            F.explode(F.col("matches_data")).alias("match")
        ).select(
            F.col("player_id"),
            F.col("match.id").alias("match_id"),
            F.col("match.league").alias("league"),
            F.col("match.season").alias("season"),
            F.to_timestamp(F.col("match.date"), "yyyy-MM-dd").alias("match_date"),
            F.col("match.team.title").alias("team"),
            F.col("match.opponent.title").alias("opponent"),
            F.col("match.goals").alias("goals"),
            F.col("match.assists").alias("assists"),
            F.col("match.minutes").alias("minutes"),
            F.col("match.shots").alias("shots"),
            F.col("match.xG").alias("xG"),
            F.col("match.xA").alias("xA"),
            F.col("match.position").alias("position"),
            F.col("match.yellow_card").alias("yellow_card"),
            F.col("match.red_card").alias("red_card"),
            F.col("match.key_passes").alias("key_passes")
        )
        
        # Aggregate player stats by season
        player_seasons_df = player_matches_df.groupBy(
            "player_id", "season", "league", "team"
        ).agg(
            F.count("match_id").alias("matches"),
            F.sum("minutes").alias("minutes"),
            F.sum("goals").alias("goals"),
            F.sum("assists").alias("assists"),
            F.sum("shots").alias("shots"),
            F.round(F.sum("xG"), 2).alias("xG"),
            F.round(F.sum("xA"), 2).alias("xA"),
            F.sum("key_passes").alias("key_passes"),
            F.sum("yellow_card").alias("yellow_cards"),
            F.sum("red_card").alias("red_cards")
        ).withColumn(
            "goals_per_90", F.round(F.col("goals") * 90 / F.col("minutes"), 2)
        ).withColumn(
            "assists_per_90", F.round(F.col("assists") * 90 / F.col("minutes"), 2)
        ).withColumn(
            "xG_per_90", F.round(F.col("xG") * 90 / F.col("minutes"), 2)
        ).withColumn(
            "xA_per_90", F.round(F.col("xA") * 90 / F.col("minutes"), 2)
        )
        
        # Write processed data
        self._write_data(player_base_df, "players")
        self._write_data(player_matches_df, "player_matches")
        self._write_data(player_seasons_df, "player_seasons")
        logger.info("Player data processing complete")


class TeamProcessor(BaseSparkProcessor):
    """Process team data"""
    
    def process(self) -> None:
        """Process team data from the raw source"""
        logger.info("Processing team data")
        
        # Read raw team data
        df = self._read_data("teams")
        
        # Process team data
        team_df = df.select(
            F.col("team_id"),
            F.col("league"),
            F.col("season"),
            F.col("team_data")
        )
        
        # Extract team base info
        team_base_df = team_df.select(
            F.col("team_id"),
            F.col("league"),
            F.col("season"),
            F.col("team_data.title").alias("name")
        )
        
        # Process team players data
        team_players_df = df.select(
            F.col("team_id"),
            F.col("league"),
            F.col("season"),
            F.explode(F.col("players_data")).alias("player")
        ).select(
            F.col("team_id"),
            F.col("league"),
            F.col("season"),
            F.col("player.id").alias("player_id"),
            F.col("player.player_name").alias("player_name"),
            F.col("player.position").alias("position"),
            F.col("player.games").alias("games"),
            F.col("player.time").alias("minutes"),
            F.col("player.goals").alias("goals"),
            F.col("player.assists").alias("assists"),
            F.col("player.shots").alias("shots"),
            F.col("player.xG").alias("xG"),
            F.col("player.xA").alias("xA"),
            F.col("player.yellow_cards").alias("yellow_cards"),
            F.col("player.red_cards").alias("red_cards")
        )
        
        # Process team matches data
        team_matches_df = df.select(
            F.col("team_id"),
            F.col("league"),
            F.col("season"),
            F.explode(F.map_values(F.col("dates_data"))).alias("matches")
        ).select(
            F.col("team_id"),
            F.col("league"),
            F.col("season"),
            F.explode(F.col("matches")).alias("match")
        ).select(
            F.col("team_id"),
            F.col("league"),
            F.col("season"),
            F.col("match.id").alias("match_id"),
            F.col("match.h.title").alias("home_team"),
            F.col("match.a.title").alias("away_team"),
            F.col("match.goals.h").alias("home_goals"),
            F.col("match.goals.a").alias("away_goals"),
            F.col("match.xG.h").alias("home_xG"),
            F.col("match.xG.a").alias("away_xG"),
            F.to_timestamp(F.col("match.datetime"), "yyyy-MM-dd HH:mm:ss").alias("match_datetime")
        )
        
        # Add team perspective columns
        home_team_matches = team_matches_df.filter(
            F.col("home_team") == F.col("name")
        ).withColumn(
            "is_home", F.lit(True)
        ).withColumnRenamed("home_goals", "goals_for"
        ).withColumnRenamed("away_goals", "goals_against"
        ).withColumnRenamed("home_xG", "xG_for"
        ).withColumnRenamed("away_xG", "xG_against"
        ).withColumn(
            "opponent", F.col("away_team")
        )
        
        away_team_matches = team_matches_df.filter(
            F.col("away_team") == F.col("name")
        ).withColumn(
            "is_home", F.lit(False)
        ).withColumnRenamed("away_goals", "goals_for"
        ).withColumnRenamed("home_goals", "goals_against"
        ).withColumnRenamed("away_xG", "xG_for"
        ).withColumnRenamed("home_xG", "xG_against"
        ).withColumn(
            "opponent", F.col("home_team")
        )
        
        # Union home and away perspectives
        team_perspective_matches = home_team_matches.union(away_team_matches)
        
        # Add result columns
        team_perspective_matches = team_perspective_matches.withColumn(
            "won", F.when(F.col("goals_for") > F.col("goals_against"), 1).otherwise(0)
        ).withColumn(
            "drawn", F.when(F.col("goals_for") == F.col("goals_against"), 1).otherwise(0)
        ).withColumn(
            "lost", F.when(F.col("goals_for") < F.col("goals_against"), 1).otherwise(0)
        ).withColumn(
            "points", F.when(F.col("goals_for") > F.col("goals_against"), 3).when(F.col("goals_for") == F.col("goals_against"), 1).otherwise(0)
        )
        
        # Write processed data
        self._write_data(team_base_df, "teams")
        self._write_data(team_players_df, "team_players")
        self._write_data(team_perspective_matches, "team_matches")
        logger.info("Team data processing complete")


class ProcessorFactory:
    """Factory for creating data processors"""
    
    @staticmethod
    def create_processor(processor_type: str, config: PipelineConfig) -> BaseSparkProcessor:
        """Create a processor of the specified type"""
        if processor_type == "league":
            return LeagueProcessor(config)
        elif processor_type == "match":
            return MatchProcessor(config)
        elif processor_type == "player":
            return PlayerProcessor(config)
        elif processor_type == "team":
            return TeamProcessor(config)
        else:
            raise ValueError(f"Unknown processor type: {processor_type}")