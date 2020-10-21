# initialize findspark for local
# import findspark
# findspark.init("/usr/local/Cellar/apache-spark/3.0.1/libexec")

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType, LongType
import logging
import boto3
from botocore.exceptions import ClientError

# read config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.1 pyspark-shell'

# # set paths to get to pyspark and java
# pyspark_submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
# if not "pyspark-shell" in pyspark_submit_args: pyspark_submit_args += " pyspark-shell"
# os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

# spark_home = os.environ.get('SPARK_HOME', None)
# sys.path.insert(0, spark_home + "/python")

# # Add the py4j to the path.
# sys.path.insert(0, os.path.join(spark_home, "/Users/johnrick/opt/spark-2.4.7-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip"))

# # Initialize PySpark
# exec(open(os.path.join(spark_home, "/Users/johnrick/Downloads/spark-3.0.0-preview2-bin-hadoop2.7/python/pyspark/python/pyspark/shell.py")).read())


def create_spark_session():
    """
    creates sparks session 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Gets the data from s3 bucket, processes the data, inserts into personal s3 bucket
    -spark is the spark input
    -input_data is the path to the udacity S3
    -output_data is the path to my S3
    """
    # get filepath to song data
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # create song_schema
    song_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select("song_id",
                       "title", 
                       "artist_id",
                       "year",
                       "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(["year","artist_id"]).format("parquet").mode("overwrite").save(output_data + "songs")
    
    # extract column to get artists_table
    artists_table = df.select("artist_id",
                         col("artist_name").alias("name"),
                         col("artist_location").alias("location"),
                         col("artist_latitude").alias("latitude"),
                         col("artist_longitude").alias("longitude")).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.format("parquet").mode("overwrite").save(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Gets the data from s3 bucket, processes the data, inserts into personal s3 bucket
    Gets data from our s3 bucket, joins with time table, and adds info to our s3 bucket
    -spark is the spark input
    -input_data is the path to the udacity S3
    -output_data is the path to my S3
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # create schema
    log_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), False),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), False),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), False),
        StructField("location", StringType(), True),
        StructField("method", StringType(), False),
        StructField("page", StringType(), False),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), False),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), False),
        StructField("ts", DoubleType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)])

    # read log data file
    log_df = spark.read.json(log_data, schema = log_schema)
    
    # filter by actions for song plays
    log_df = log_df.where(log_df.page == "NextSong")
    
    # create users table schema
    users_table = log_df.select(col("userId").alias("user_id"),
                       col("firstName").alias("first_name"),
                       col("lastName").alias("last_name"),
                       "gender",
                       "level").dropDuplicates()
    # write users data to s3
    users_table.write.format("parquet").mode("overwrite").save(output_data+"users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    log_df = log_df.withColumn("datetime", get_datetime(log_df.ts))
    
    # extract columns to create time table
    time_table = log_df.select(
        log_df.timestamp.alias('start_time'),
        hour(log_df.datetime).alias('hour'),
        dayofmonth(log_df.datetime).alias('day'),
        weekofyear(log_df.datetime).alias('week'),
        month(log_df.datetime).alias('month'),
        year(log_df.datetime).alias('year'),
        date_format(log_df.datetime, 'u').alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").format("parquet").save(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"songs/*/*/*")
    
    # read in artist data to use for songplays table
    artist_df = spark.read.parquet(output_data + "artists/*")
    
    # join tables
    songs_logs = log_df.join(songs_table, (log_df.song == song_df.title))
    artists_songs_logs = songs_logs.join(artist_df, (songs_logs.artist == artists_df.name))
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = artists_songs_logs.join(
        time_table,
        artists_songs_logs.timestamp == time_table.start_time, 'left')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.format("parquet").mode("overwrite").save(output_data + "songplays")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ud1-s3datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
