import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: spark-submit spark_kafka_to_local.py <hostname> <port> <topic>")
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]

    spark = SparkSession  \
	    .builder  \
	    .appName("CapstoneProject") \
	    .getOrCreate()
    spark.conf.set("stopGracefullyOnShutdown", "true")
    spark.sparkContext.setLogLevel('ERROR')

    bootstrap_server = host + ":" + port

    raw_data = spark  \
	    .readStream  \
	    .format("kafka")  \
	    .option("kafka.bootstrap.servers", bootstrap_server)  \
	    .option("subscribe", topic)  \
	    .option("startingOffsets", "earliest") \
	    .load()

    schema = StructType() \
            .add("customer_id", StringType()) \
            .add("app_version", StringType()) \
            .add("OS_version", StringType()) \
            .add("lat", StringType()) \
            .add("lon", StringType()) \
            .add("page_id", StringType()) \
            .add("button_id", StringType()) \
            .add("is_button_click", StringType()) \
            .add("is_page_view", StringType()) \
            .add("is_scroll_up", StringType()) \
            .add("is_scroll_down", StringType()) \
            .add("timestamp\n", TimestampType())

    raw_data = raw_data.selectExpr("cast(value as string)") \
	    .select(from_json("value", schema).alias("temp")).select("temp.*") \
	    .withColumnRenamed("timestamp\n", "timestamp")

    console_output = raw_data.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "true") \
            .start()

    csv_output = raw_data.writeStream \
            .format("csv") \
            .outputMode("append") \
            .option("truncate", "false") \
            .option("path", "/user/ec2-user/capstone_project/warehouse/capstone") \
            .option("checkpointLocation", "/user/ec2-user/capstone_project/warehouse/checkpoint") \
            .start()

    console_output.awaitTermination(900)
    csv_output.awaitTermination()

