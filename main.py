from loguru import logger
from postgresql_database import create_database, create_tables
from kaggle_downloader import download_ashrae_files,delete_ashrae_files
from kafka_producer import produce_to_kafka
import config

def main():
    """
    Main entry point for the data pipeline.
    This script creates the database and tables, downloads CSV files from Kaggle,
    and then streams the CSV data to the appropriate Kafka topics.
    """
    logger.info("Starting the data pipeline: setting up the database, downloading CSV files, and streaming data to Kafka.")

    try:
        # Step 1: Set up the database and tables.
        create_database()
        create_tables()

        # Step 2: Download CSV files from Kaggle.
        download_ashrae_files()

        # Step 3: Stream CSV data to Kafka.
        logger.info("Streaming CSV data to Kafka...")
        produce_to_kafka("data/building_metadata.csv", config.KAFKA_BUILDING_METADATA_TOPIC)
        produce_to_kafka("data/train.csv", config.KAFKA_TRAIN_TOPIC)
        produce_to_kafka("data/weather_train.csv", config.KAFKA_WEATHER_TOPIC)

        logger.info("Data pipeline completed: Database set up, CSV files downloaded, and data streamed to Kafka successfully.")
    except Exception as e:
        logger.error("An error occurred during the data pipeline execution: {}", e)
    delete_ashrae_files()
if __name__ == "__main__":
    main()
