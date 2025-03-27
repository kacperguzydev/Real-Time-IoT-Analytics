import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import config

# Initialize Spark with Kafka and PostgreSQL JDBC packages.
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.2.20"
    ) \
    .getOrCreate()

# Define JSON schemas for each data stream.
raw_building_schema = StructType([
    StructField("site_id", StringType(), True),
    StructField("building_id", StringType(), True),
    StructField("primary_use", StringType(), True),
    StructField("square_feet", StringType(), True),
    StructField("year_built", StringType(), True),
    StructField("floor_count", StringType(), True)
])

raw_train_schema = StructType([
    StructField("building_id", StringType(), True),
    StructField("meter", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("meter_reading", StringType(), True)
])

raw_weather_schema = StructType([
    StructField("site_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("air_temperature", StringType(), True),
    StructField("cloud_coverage", StringType(), True),
    StructField("dew_temperature", StringType(), True),
    StructField("precip_depth_1_hr", StringType(), True),
    StructField("sea_level_pressure", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("wind_speed", StringType(), True)
])

# Combine topics from configuration.
topics = [
    config.KAFKA_BUILDING_METADATA_TOPIC,
    config.KAFKA_TRAIN_TOPIC,
    config.KAFKA_WEATHER_TOPIC
]
topics_str = ",".join(topics)

# Read data from Kafka as a stream.
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config.KAFKA_BROKER) \
    .option("subscribe", topics_str) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert binary Kafka 'value' to string and keep the topic field.
kafka_df = df.selectExpr("CAST(value AS STRING) as json_str", "topic")

# Parse the JSON for each stream.
df_building_raw = kafka_df.filter(col("topic") == config.KAFKA_BUILDING_METADATA_TOPIC) \
    .select(from_json(col("json_str"), raw_building_schema).alias("data")).select("data.*")

df_train_raw = kafka_df.filter(col("topic") == config.KAFKA_TRAIN_TOPIC) \
    .select(from_json(col("json_str"), raw_train_schema).alias("data")).select("data.*")

df_weather_raw = kafka_df.filter(col("topic") == config.KAFKA_WEATHER_TOPIC) \
    .select(from_json(col("json_str"), raw_weather_schema).alias("data")).select("data.*")

# Function to clean and cast columns.
def clean_cast(col_name, target_type):
    return when((col(col_name).isNull()) | (col(col_name) == "") | (col(col_name) == "null"), None) \
        .otherwise(col(col_name)).cast(target_type)

# Process building metadata.
df_building = df_building_raw \
    .withColumn("site_id", clean_cast("site_id", IntegerType())) \
    .withColumn("building_id", clean_cast("building_id", IntegerType())) \
    .withColumn("square_feet", clean_cast("square_feet", DoubleType())) \
    .withColumn("year_built", clean_cast("year_built", IntegerType())) \
    .withColumn("floor_count", clean_cast("floor_count", IntegerType()))
df_building = df_building.na.drop(subset=["building_id"])

# Process train data.
df_train = df_train_raw \
    .withColumn("building_id", clean_cast("building_id", IntegerType())) \
    .withColumn("meter", clean_cast("meter", IntegerType())) \
    .withColumn("timestamp", clean_cast("timestamp", TimestampType())) \
    .withColumn("meter_reading", clean_cast("meter_reading", DoubleType()))
df_train = df_train.na.drop(subset=["building_id", "timestamp"])

# Process weather data.
df_weather = df_weather_raw \
    .withColumn("site_id", clean_cast("site_id", IntegerType())) \
    .withColumn("timestamp", clean_cast("timestamp", TimestampType())) \
    .withColumn("air_temperature", clean_cast("air_temperature", DoubleType())) \
    .withColumn("cloud_coverage", clean_cast("cloud_coverage", DoubleType())) \
    .withColumn("dew_temperature", clean_cast("dew_temperature", DoubleType())) \
    .withColumn("precip_depth_1_hr", clean_cast("precip_depth_1_hr", DoubleType())) \
    .withColumn("sea_level_pressure", clean_cast("sea_level_pressure", DoubleType())) \
    .withColumn("wind_direction", clean_cast("wind_direction", DoubleType())) \
    .withColumn("wind_speed", clean_cast("wind_speed", DoubleType()))
df_weather = df_weather.na.drop(subset=["site_id", "timestamp"])

# Write function to push data into PostgreSQL via JDBC.
def write_to_postgres(df, epoch_id, table_name):
    batch_count = df.count()
    print(f"Processing batch {epoch_id} with {batch_count} records for '{table_name}'")
    df.show(5, truncate=False)

    jdbc_url = config.JDBC_URL

    # Define unique key columns for each table.
    unique_keys = {
        "building_metadata": ["building_id"],
        "train_data": ["building_id", "meter", "timestamp"],
        "weather_train": ["site_id", "timestamp"]
    }

    # First, remove duplicates within the micro-batch.
    if table_name in unique_keys:
        df = df.dropDuplicates(unique_keys[table_name])

    # Next, check the target table for existing keys.
    if table_name in unique_keys:
        try:
            import psycopg2
            import pandas as pd
            conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD
            )
            if table_name == "building_metadata":
                query = "SELECT building_id FROM building_metadata"
            elif table_name == "train_data":
                query = "SELECT building_id, meter, timestamp FROM train_data"
            elif table_name == "weather_train":
                query = "SELECT site_id, timestamp FROM weather_train"
            else:
                query = f"SELECT * FROM {table_name}"
            existing = pd.read_sql(query, conn)
            conn.close()
            print(f"Fetched {len(existing)} existing records for table '{table_name}'.")
        except Exception as e:
            print(f"Error fetching existing keys for {table_name}: {e}")
            existing = None

        if existing is not None and not existing.empty:
            # Create a composite key column in the Spark DataFrame.
            key_cols = unique_keys[table_name]
            df = df.withColumn("key", concat_ws("_", *[col(c).cast("string") for c in key_cols]))
            # Create a composite key in the pandas DataFrame.
            existing["key"] = existing.apply(lambda row: "_".join([str(row[c]) for c in key_cols]), axis=1)
            existing_keys = set(existing["key"].tolist())
            # Filter out rows with keys that already exist.
            df = df.filter(~col("key").isin(list(existing_keys)))
            # Drop the temporary key column.
            df = df.drop("key")

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config.DB_USER) \
        .option("password", config.DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start streaming data into PostgreSQL using foreachBatch.
query_building = df_building.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "building_metadata")) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/building_metadata") \
    .start()

query_train = df_train.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "train_data")) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/train_data") \
    .start()

query_weather = df_weather.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "weather_train")) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/weather_train") \
    .start()

spark.streams.awaitAnyTermination()
