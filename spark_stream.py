import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
import pandas as pd

# Configure logging for the application
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

# Function to create a keyspace in Cassandra
def create_keyspace(session):
    # Execute CQL to create a keyspace if it doesn't exist
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                    """)
    print("Keyspace created successfully!")

# Function to create a table in Cassandra
def create_table(session):
    # Execute CQL to create a table for data updates
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.data_update (
                    timestamp TEXT PRIMARY KEY,
                    volume_swap TEXT, 
                    close_swap TEXT,
                    high_swap TEXT,
                    low_swap TEXT,
                    open_swap TEXT,
                    amount_swap TEXT
                    )
                    """)
    print("Table created successfully!")

# Function to insert data into Cassandra
def insert_data(session, **kwargs):
    print("inserting data...")
    # Extracting data from keyword arguments
    timestamp = kwargs.get("timestamp")
    volume_swap = kwargs.get("volume_swap")
    close_swap = kwargs.get("close_swap")
    high_swap = kwargs.get("high_swap")
    low_swap = kwargs.get("low_swap")
    open_swap = kwargs.get("open_swap")
    amount_swap = kwargs.get("amount_swap")

    # Insert data into Cassandra table, handle exceptions
    try:
        session.execute("""
                        INSERT INTO spark_stream.data_update(
                        timestamp, volume_swap, close_swap, high_swap, low_swap, open_swap, amount_swap)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        """, (timestamp, volume_swap, close_swap, high_swap, low_swap, open_swap, amount_swap))
        logging.info(f"Data inserted at {timestamp}")
    except Exception as e:
        logging.error(f"Could not insert data due to {e}")

# Function to create Spark connection
def create_spark_connection():
    """
    Creates the Spark Session with appropriate configuration for Cassandra and Kafka connectors.
    """
    try:
        # Configuring SparkSession with Cassandra and Kafka connectors
        s_conn = SparkSession.builder \
                 .appName('SparkDataStreaming') \
                 .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
                 .config("spark.cassandra.connection.host", "localhost") \
                 .config("spark.cassandra.connection.port","9042")\
                 .config("spark.cassandra.auth.username", "cassandra") \
                 .config("spark.cassandra.auth.password", "cassandra") \
                 .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")

    except Exception as e:
        logging.error(f'Impossible to connect, an error occurred: {e}')

    return s_conn

# Function to connect to Kafka using Spark
def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        # Reading data from Kafka into a Spark DataFrame
        spark_df = spark_conn.readStream \
                   .format('kafka') \
                   .option('kafka.bootstrap.servers', 'localhost:9092,broker:29092') \
                   .option('subscribe', 'data_update') \
                   .option('startingOffsets', 'latest') \
                   .option("failOnDataLoss", "false") \
                   .load()

        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.error(f"kafka dataframe could not be created because: {e}")

    return spark_df

# Function to create a connection to Cassandra
def create_cassandra_connection():
    try:
        # Connecting to Cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Impossible to create cassandra connection due to {e}")
        return None

# Function to process data from Kafka using Spark
def create_selection_df_from_kafka(spark_df):
    # Defining the schema for JSON data
    json_schema = StructType([
        StructField("schema", StringType()), 
        StructField("data", ArrayType(StructType([
            StructField("timestamp", TimestampType()),
            StructField("volume_swap", StringType()),
            StructField("close_swap", StringType()),
            StructField("high_swap", StringType()),
            StructField("low_swap", StringType()),
            StructField("open_swap", StringType()),
            StructField("amount_swap", StringType())
        ])))
    ])

    if spark_df is not None:
        # Processing the DataFrame to select the required fields
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
                      .select(from_json(col('value'), json_schema).alias('parsed_json')) \
                      .select(explode(col("parsed_json.data")).alias("data")) \
                      .select("data.*")

        return sel
    else:
        print(f"spark_df is: {spark_df}")
        return None

if __name__ == "__main__":
    # Main execution block
    # Creating Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connecting to Kafka and processing the data
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        # Creating Cassandra connection
        session = create_cassandra_connection()

        if session is not None:
            # Creating keyspace and table in Cassandra
            create_keyspace(session)
            create_table(session)
            # Streaming data from Spark to Cassandra
            streaming_query = selection_df.writeStream \
                             .format("org.apache.spark.sql.cassandra") \
                             .option("checkpointLocation", "/tmp/checkpoint") \
                             .option("keyspace", "spark_streams") \
                             .option("table", "data_update") \
                             .trigger(processingTime='15 seconds')\
                             .start()
            streaming_query.awaitTermination()
