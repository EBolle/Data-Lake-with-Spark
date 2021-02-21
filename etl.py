import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as sql_f
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from schemas import log_schema, song_schema


config = configparser.ConfigParser()
config.read('dl.cfg')

song_data_path = config['song_data_path']
log_data_path = config['log_data_path']
output_data_path = config['output_data_path']


def main():
    """
    Main execution method which sequentially calls the methods to execute the full Sparify ETL pipeline:
    - Get or create a Spark session
    - Extract the song data from S3
    - Transform the song data to 2 tables: songs and artists
    - Extract the log data from S3
    - Transform the log data to 3 tables: time, users, and songplays
    - Load the 5 tables into a separate analytical folder on S3 in parquet format
    """
    spark = create_spark_session()

    process_song_data(spark, song_data_path, output_data_path, song_schema)
    process_log_data(spark, song_data_path, output_data_path, log_schema)

    spark.stop()


def create_spark_session():
    """Gets or creates an activate Spark session."""
    spark = (SparkSession
        .builder
        .appName("Sparkify ETL")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .getOrCreate()
    )

    return spark


def process_song_data(spark, input_data: str, output_data: str, schema: StructType):
    """
    Import the song .json files on S3 as input, transforms them into 2 separate spark dataFrames,
    and exports the tables back to S3 as Parquet files in separate folders.
    """
    song_data = spark.read.json(path=song_data_path, schema=song_schema)

    songs_df = (song_data
        .select(['song_id', 'title', 'artist_id', 'year', 'duration'])
        .repartition(8, 'year', 'artist_id')
    )

    songs_df_write = (songs_df
        .write
        .mode("overwrite")
        .partitionBy('year', 'artist_id')
        .parquet(f"{output_data_path}/songs")
    )

    artists_df = (song_data
        .select(col('artist_id'),
                col('artist_name').alias('name'),
                col('artist_location').alias('location'),
                col('artist_latitude').alias('latitude'),
                col('artist_longitude').alias('longitude'))
        .withColumn('row_number', sql_f.row_number().over(Window.partitionBy('artist_id').orderBy('name')))
        .where('row_number = 1')
        .drop('row_number')
    )

    artists_df_write = (artists_df
        # .repartition(8)
        .write
        .mode('overwrite')
        .parquet(f"{output_data_path}/artists")
    )


def process_log_data(spark, input_data, output_data, schema: StructType):
    """DOCSTRING"""
    pass


if __name__ == "__main__":
    main()
