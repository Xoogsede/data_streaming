import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
import pandas as pd


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

def create_keyspace(session):
    # create keyspace in cassandra
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                    """)
    print("Keyspace created successfully!")

def create_table(session):
    # create table in cassandra
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

def insert_data(session, **kwargs):
    print("inserting data...")

    timestamp = kwargs.get("timestamp")
    volume_swap = kwargs.get("volume_swap")
    close_swap = kwargs.get("close_swap")
    high_swap = kwargs.get("high_swap")
    low_swap = kwargs.get("low_swap")
    open_swap = kwargs.get("open_swap")
    amount_swap = kwargs.get("amount_swap")

    try:
        session.execute("""
                        INSERT INTO spark_stream.data_update(
                        timestamp, volume_swap, close_swap, high_swap, low_swap, open_swap, amount_swap)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        """, (timestamp, volume_swap, close_swap, high_swap, low_swap, open_swap, amount_swap))
        logging.info(f"Data inserted at {timestamp}")
    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


def create_spark_connection():
    """
    Creates the Spark Session with suitable configs.
    """
    try:
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
        logging.error(f'Impossible to connect, an error occured: {e}')

    return s_conn
    
def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
                   .format('kafka') \
                   .option('kafka.bootstrap.servers', 'localhost:9092,broker:29092') \
                   .option('subscribe', 'data_update') \
                   .option('startingOffsets', 'latest') \
                   .option("failOnDataLoss", "false") \
                   .load()

        logging.info("kafka datadrame created successfully")
    except Exception as e:
        logging.error(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
            
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Impossible to create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
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
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
                      .select(from_json(col('value'), json_schema).alias('parsed_json')) \
                      .select(explode(col("parsed_json.data")).alias("data")) \
                      .select("data.*")

        return sel
    else:
        print(f"spark_df is: {spark_df}")
        return None

    


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
       # connect to kafka with spark connection
       spark_df = connect_to_kafka(spark_conn)
       selection_df = create_selection_df_from_kafka(spark_df)
       session = create_cassandra_connection()

       if session is not None:
           create_keyspace(session)
           create_table(session)
        #    insert_data(session)
           
           streaming_query = selection_df.writeStream \
                             .format("org.apache.spark.sql.cassandra") \
                             .option("checkpointLocation", "/tmp/checkpoint") \
                             .option("keyspace", "spark_streams") \
                             .option("table", "data_update") \
                             .trigger(processingTime='15 seconds')\
                             .start()
           streaming_query.awaitTermination()
