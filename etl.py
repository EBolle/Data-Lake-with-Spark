import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window
import pyspark.sql.functions as sql_f

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Gets or creates an activate Spark session."""
    spark = (
        SparkSession
        .builder
        .appName("Sparkify ETL")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .getOrCreate()
    )
    return spark


def process_song_data(spark, input_data: str, output_data: str):
    """
    Import the song .json files on S3 as input, transforms them into 2 separate spark dataFrames,
    and exports the tables back to S3 as Parquet files in separate folders.
    """
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract the columns and write them to parquet files
    songs_table = (
        df
        .select(['song_id', 'title', 'artist_id', 'year', 'duration'])
        .repartition(200)
        .write
        .mode("overwrite")
        .partitionBy('year', 'artist_id')
        .parquet(f"{output_data}/songs")
    )

    # create a list of the right column aliases
    artists_list = (
        df
        .select(col('artist_id'),
                col('artist_name').alias('artist'),
                col('artist_location').alias('location'),
                col('artist_latitude').alias('latitude'),
                col('artist_longitude').alias('longitude'))
        .collect()
    )
    
    # write artists table to parquet files
    artists_df = spark.createDataFrame(artists_list)

    # make sure artists_df is unique
    artists_df = (
        artists_df
        .withColumn('row_number', sql_f.row_number().over(Window.partitionBy(artists_df['artist_id']).orderBy(artists_df['artist'])))
        .where('row_number = 1')
        .collect()
    )

    # write artists to parquet files
    artists_table = (
        artists_df
        .repartition(200)
        .write
        .mode('overwrite')
        .partitionBy('artist_id')
        .parquet(f"{output_data}/artists")
    )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://<INSERT URL HERE>/analytical"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
