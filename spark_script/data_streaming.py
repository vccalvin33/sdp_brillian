import signal
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, current_timestamp, from_json, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, TimestampType, ArrayType


CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'trading'
CASSANDRA_TABLE = 'real_time_data'

MYSQL_HOST = 'mysql'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'trading'
MYSQL_TABLE = 'aggregated_data'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'root'

KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'trading_data'

def write_to_cassandra(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
        .mode("append") \
        .save()

def write_to_mysql(df, epoch_id):
    agg_df = df.withColumn("date", to_date(col("created_at"))) \
        .groupBy("date", "sector") \
        .agg(sum("volume").alias("total_volume")) \
        .withColumn("processed_at", current_timestamp())

    agg_df.write \
        .jdbc(url=f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}", 
                table=MYSQL_TABLE, 
                mode="append", 
                properties= {
                    "driver": "com.mysql.cj.jdbc.Driver",
                    "user": MYSQL_USERNAME,
                    "password": MYSQL_PASSWORD}) 

def signal_handler(signal, frame):
    print("Waiting for 90 seconds before terminating the Spark Streaming application...")
    time.sleep(90)
    sys.exit(0)

def main():
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra-MySQL") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("ticker", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("market_cap", LongType(), True),
        StructField("volume", LongType(), True),
        StructField("sector", StringType(), True)
    ]))

    df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

    df = df.selectExpr("CAST(value AS STRING) as json_data")
    df = df.withColumn("data", from_json(col("json_data"), schema))
    df_exploded = df.select(explode(col("data")).alias("exploded_data"))
    tmp_df = df_exploded.select("exploded_data.*")
    tmp_df = tmp_df.withColumn("created_at", col("created_at").cast(TimestampType()))

    tmp_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start() 

    tmp_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .trigger(processingTime='60 seconds') \
        .start() 

    signal.signal(signal.SIGINT, signal_handler)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()