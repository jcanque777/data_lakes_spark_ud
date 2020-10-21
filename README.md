# Project: Data Lake

## Introduction
Sparkify wants to move their data warehouse to a data lake. The objective of the project is to build an ETL pipeline that extracts their data, processes using Spark, load data back into s3 with tables saved in parquet format.

An ETL pipeline is created that takes raw data from S3, transforms and creates tables in spark, and uploads data to another s3 bucket with the correct table schema.


## ETL pipeline
First create an S3 bucket named 'ud1-s3datalake'. This will be the S3 bucket where final tables are loaded. The variable output_data matches this address.

### create_spark_session
This creates a spark session to connect to spark. We use algorithm version 2 for mapreduce to speed up process. This changes the algorithm to complete when files are finished. Algorithm 1 is the default and will look at files individually, causing a much slower process.

### process_song_data
We get the raw JSON song data from the S3 bucket with the correct data structures. We then use the data to create the songs_table and artists_table. Finally, the tables are saved to our new S3 bucket in parquet format. 

### process_log_data
We get the raw JSON log data from the S3 bucket with the correct data structures. The data is filtered where page is "NextSong". Users table is created and saved to our S3 bucket in parquet format.

We create a time table using the ts column to extract time and date information. The table is then saved to our S3 bucket with the parquet format. 

Song and artist tables are read from our S3 bucket and joined to create songplays table. 

## How to run python scripts
1. Fill in login information in provided dl.cfg file
2. Enter and run in jupyter notebook: !python etl.py

## Conclusion
The project allows user to exercise patience if using a MacOS. Out of the box, Jupyter Notebook, Spark, Java, S3, and Mac do not play along well without needing to point each to the right direction.

Saving data to parquet format was a good practice for columnar storage.
