import os
import psycopg2
from psycopg2 import sql
from loguru import logger
import config

def create_database():
    """
    Connects to the default 'postgres' database and creates our target database if it doesn't already exist.
    """
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database="postgres",
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()
        # Check if the target database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.DB_NAME,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(config.DB_NAME)))
            logger.info("Database '{}' created.", config.DB_NAME)
        else:
            logger.info("Database '{}' already exists.", config.DB_NAME)
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("Failed to create database: {}", e)
        raise

def get_connection():
    """
    Establishes and returns a connection to the target PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        logger.info("Connected to database '{}'.", config.DB_NAME)
        return conn
    except Exception as e:
        logger.error("Could not connect to PostgreSQL: {}", e)
        raise

def create_tables():
    """
    Creates the necessary tables for train data, building metadata, and weather data.
    Primary keys are added to handle duplicates.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Create table for train data with a composite primary key (building_id, meter, timestamp)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS train_data (
                building_id INT,
                meter INT,
                timestamp TIMESTAMP,
                meter_reading DOUBLE PRECISION,
                PRIMARY KEY (building_id, meter, timestamp)
            );
        """)
        # Create table for building metadata with building_id as the primary key.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS building_metadata (
                building_id INT PRIMARY KEY,
                site_id INT,
                primary_use VARCHAR(255),
                square_feet DOUBLE PRECISION,
                year_built INT,
                floor_count INT
            );
        """)
        # Create table for weather data with a composite primary key (site_id, timestamp)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_train (
                site_id INT,
                timestamp TIMESTAMP,
                air_temperature DOUBLE PRECISION,
                cloud_coverage DOUBLE PRECISION,
                dew_temperature DOUBLE PRECISION,
                precip_depth_1_hr DOUBLE PRECISION,
                sea_level_pressure DOUBLE PRECISION,
                wind_direction DOUBLE PRECISION,
                wind_speed DOUBLE PRECISION,
                PRIMARY KEY (site_id, timestamp)
            );
        """)
        conn.commit()
        logger.info("Tables created (or they already exist).")
    except Exception as e:
        conn.rollback()
        logger.error("Error while creating tables: {}", e)
    finally:
        cur.close()
        conn.close()
