import configparser
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (col, year, month, dayofmonth, hour, weekofyear, dayofweek,
                                   broadcast, monotonically_increasing_id)
import pyspark.sql.functions as sql_f
from pyspark.sql.types import StructType, TimestampType
from pyspark.sql.window import Window
from schemas import log_schema, song_schema

config = configparser.ConfigParser()
config.read('dl.cfg')

song_data_path = config['PATHS]['song_data_path']
log_data_path = config['PATHS]['log_data_path']
output_data_path = config['PATHS]['output_data_path']


def main():
    """
    Main execution method which sequentially calls the methods to execute the full Sparify ETL pipeline:
    - Get or create a Spark session
    - Extract the raw .json song data from S3
    - Transform the song data into 2 folders - songs and artists - and Load these tables back into S3 in parquet format
    - Extract the raw .json log data from S3
    - Transform the log data into 3 folders - time, users, and songplays - and Load these tables back into S3 in parquet format
    - stop the spark session
    """
    spark = create_spark_session()

    songs_df = process_song_data(spark, song_data_path, output_data_path, song_schema)
    process_log_data(spark, song_data_path, output_data_path, log_schema, songs_df)

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


def process_song_data(spark, input_data: str, output_data: str, schema: StructType) -> DataFrame:
    """Extract the raw data from S3, transform the data to our likings, and load it back into S3 in parquet format."""
    song_data = spark.read.json(path=song_data_path, schema=song_schema)

    song_data_cached = song_data.repartition(8).cache()
    print(song_data_cached.count())

    songs_df = (song_data_cached
        .select(['song_id', 'title', 'artist_id', 'year', 'duration'])
        .repartition(8, 'year', 'artist_id')
    )

    songs_df_write = (songs_df
        .write
        .mode("overwrite")
        .partitionBy('year', 'artist_id')
        .parquet(f"{output_data_path}/analytical/songs")
    )

    artists_df = (song_data_cached
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
        .repartition(8)
        .write
        .mode('overwrite')
        .parquet(f"{output_data_path}/analytical/artists")
    )

    return songs_df


def process_log_data(spark,
                     input_data: str,
                     output_data: str,
                     schema: StructType,
                     songs_df: DataFrame) -> None:
    """Extract the raw data from S3, transform the data to our likings, and load it back into S3 in parquet format."""
    log_data = spark.read.json(path=log_data_path, schema=log_schema)

    log_data_cached = log_data.repartition(8).cache()
    print(log_data_cached.count())

    users_df = (log_data_cached
        .select(col('userId').alias('user_id'),
                col('firstName').alias('first_name'),
                col('lastName').alias('last_name'),
                col('gender'),
                col('level'),
                col('ts'))
        .withColumn('row_number', sql_f.row_number().over(Window.partitionBy('user_id').orderBy(col('ts').desc())))
        .where("row_number = 1")
        .drop('row_number', 'ts')
        .repartition(8)
    )

    users_df_write = (users_df
        .write
        .mode('overwrite')
        .parquet(f"{output_data_path}/analytical/users")
    )

    time_df = (log_data_cached
        .where("page = 'NextSong'")
        .withColumn('start_time', sql_f.from_unixtime(col('ts')/1000).cast(TimestampType()))
        .select(col('start_time'),
                hour('start_time').alias('hour'),
                dayofmonth('start_time').alias('day'),
                weekofyear('start_time').alias('week'),
                month("start_time").alias('month'),
                year("start_time").alias('year'))
        .withColumn('weekday', sql_f.when(dayofweek(col('start_time')) < 6, True).otherwise(False))
        .dropDuplicates(['start_time'])
        .repartition(8, 'year', 'month')
    )

    time_df_export = (time_df
        .write
        .mode('overwrite')
        .partitionBy('year', 'month')
        .parquet(f"{output_data_path}/analytical/time")
    )

    events_df = (log_data_cached
        .where("page = 'NextSong'")
        .withColumn("songplay_id", monotonically_increasing_id())
        .withColumn('start_time', sql_f.from_unixtime(col('ts') / 1000).cast(TimestampType()))
        .select(col('songplay_id'),
                col('start_time'),
                col('userId').alias('user_id'),
                col('level'),
                col('sessionId').alias('session_id'),
                col('location'),
                col('userAgent').alias('user_agent'),
                col('song'),
                col('length'),
                month("start_time").alias('month'),
                year("start_time").alias('year'))
        .repartition(8)
    )

    join_condition = [songs_df.title == events_df.song, songs_df.duration == events_df.length]

    songplays_df = (events_df
        .join(broadcast(songs_df.drop('year')), on=join_condition, how='left')
        .select(col('songplay_id'),
                col('start_time'),
                col('user_id'),
                col('level'),
                col('song_id'),
                col('artist_id'),
                col('session_id'),
                col('location'),
                col('user_agent'),
                col('month'),
                col('year'))
        .repartition(8, 'year', 'month')
    )

    songplays_df_write = (songplays_df
        .write
        .mode('overwrite')
        .partitionBy('year', 'month')
        .parquet(f"{output_data_path}/analytical/songplays")
    )


if __name__ == "__main__":
    main()
