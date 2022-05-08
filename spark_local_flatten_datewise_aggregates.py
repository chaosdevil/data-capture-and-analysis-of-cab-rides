import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre/"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")

try:
    spark = SparkSession  \
            .builder  \
            .appName("CapstoneProject")  \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # create a schema
    schema = StructType(
        [
            StructField("booking_id", StringType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("driver_id", IntegerType(), True),
            StructField("customer_app_version", StringType(), True),
            StructField("customer_phone_os_version", StringType(), True),
            StructField("pickup_lat", FloatType(), True),
            StructField("pickup_lon", FloatType(), True),
            StructField("drop_lat", FloatType(), True),
            StructField("drop_lon", FloatType(), True),
            StructField("pickup_timestamp", TimestampType(), True),
            StructField("drop_timestamp", TimestampType(), True),
            StructField("trip_fare", IntegerType(), True),
            StructField("tip_amount", IntegerType(), True),
            StructField("currency_code", StringType(), True),
            StructField("cab_color", StringType(), True),
            StructField("cab_registration_no", StringType(), True),
            StructField("customer_rating_by_driver", IntegerType(), True),
            StructField("rating_by_customer", IntegerType(), True),
            StructField("passenger_count", IntegerType(), True),
        ]
    )

    # load bookings data
    bookings = spark.read.csv("/user/ec2-user/capstone_project/rds-data", schema=schema)

    # load clicking stream data
    clicking_stream = spark.read.csv("/user/ec2-user/capstone_project/warehouse/capstone/*.csv", sep=",", inferSchema=True)

    # change column names
    clicking_stream = clicking_stream.withColumnRenamed("_c0", "customer_id") \
            .withColumnRenamed("_c1", "app_version") \
            .withColumnRenamed("_c2", "os_version") \
            .withColumnRenamed("_c3", "lat") \
            .withColumnRenamed("_c4", "lon") \
            .withColumnRenamed("_c5", "page_id") \
            .withColumnRenamed("_c6", "button_id") \
            .withColumnRenamed("_c7", "is_button_click") \
            .withColumnRenamed("_c8", "is_page_view") \
            .withColumnRenamed("_c9", "is_scroll_up") \
            .withColumnRenamed("_c10", "is_scroll_down") \
            .withColumnRenamed("_c11", "timestamp")

    # datewise bookings aggregates data
    datewise_bookings = bookings \
            .withColumn("date", to_date("pickup_timestamp")) \
            .groupBy("date").agg(count("date").alias("total_bookings")).orderBy("date")


    # create Hive database
    spark.sql("create database if not exists capstone_project")

    # write dataframes to Hive
    bookings.write.mode("overwrite").saveAsTable("capstone_project.bookings")
    clicking_stream.write.mode("overwrite").saveAsTable("capstone_project.clicking_stream")
    datewise_bookings.write.format("orc").mode("overwrite").saveAsTable("capstone_project.datewise_bookings")
    print("Successfully loaded dataframes to Hive")
except Exception as e:
    print("Error somewhere!")
    print(e)
    sys.exit()

