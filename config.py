import os

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_BUILDING_METADATA_TOPIC = os.getenv("KAFKA_BUILDING_METADATA_TOPIC", "building_metadata_topic")
KAFKA_TRAIN_TOPIC = os.getenv("KAFKA_TRAIN_TOPIC", "train_topic")
KAFKA_WEATHER_TOPIC = os.getenv("KAFKA_WEATHER_TOPIC", "weather_topic")

# PostgreSQL configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "IoT_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Maximum records to process (e.g. for CSV imports)
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "10000"))
