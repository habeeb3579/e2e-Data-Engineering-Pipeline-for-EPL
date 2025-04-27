-- Create schema for raw data
CREATE SCHEMA IF NOT EXISTS raw;

-- Create schema for staging data
CREATE SCHEMA IF NOT EXISTS staging;

-- Create schema for analytical models
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create leagues table
CREATE TABLE IF NOT EXISTS raw.leagues (
    id SERIAL PRIMARY KEY,
    league_name VARCHAR(100) NOT NULL,
    league_code VARCHAR(10) NOT NULL,
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create seasons table
CREATE TABLE IF NOT EXISTS raw.seasons (
    id SERIAL PRIMARY KEY,
    season_name VARCHAR(20) NOT NULL,
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create teams table
CREATE TABLE IF NOT EXISTS raw.teams (
    id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL,
    team_code VARCHAR(10),
    league_id INTEGER REFERENCES raw.leagues(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create players table
CREATE TABLE IF NOT EXISTS raw.players (
    id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL,
    player_id VARCHAR(50) NOT NULL,
    team_id INTEGER REFERENCES raw.teams(id),
    position VARCHAR(20),
    nationality VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create matches table
CREATE TABLE IF NOT EXISTS raw.matches (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL,
    league_id INTEGER REFERENCES raw.leagues(id),
    season_id INTEGER REFERENCES raw.seasons(id),
    home_team_id INTEGER REFERENCES raw.teams(id),
    away_team_id INTEGER REFERENCES raw.teams(id),
    match_date TIMESTAMP,
    home_score INTEGER,
    away_score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create match_stats table
CREATE TABLE IF NOT EXISTS raw.match_stats (
    id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES raw.matches(id),
    home_xg DECIMAL(10,2),
    away_xg DECIMAL(10,2),
    home_shots INTEGER,
    away_shots INTEGER,
    home_shots_on_target INTEGER,
    away_shots_on_target INTEGER,
    home_deep INTEGER,
    away_deep INTEGER,
    home_ppda DECIMAL(10,2),
    away_ppda DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create player_stats table
CREATE TABLE IF NOT EXISTS raw.player_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER REFERENCES raw.players(id),
    match_id INTEGER REFERENCES raw.matches(id),
    mins_played INTEGER,
    goals INTEGER,
    assists INTEGER,
    shots INTEGER,
    key_passes INTEGER,
    xg DECIMAL(10,2),
    xa DECIMAL(10,2),
    position VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create ingestion_logs table
CREATE TABLE IF NOT EXISTS raw.ingestion_logs (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(100),
    records_processed INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);