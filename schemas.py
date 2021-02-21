"""This module contains the schema of the song and log data on S3."""


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType


song_schema = (StructType([
    StructField('artist_id', StringType(), True),
    StructField('artist_latitude', DoubleType(), True),
    StructField('artist_location', StringType(), True),
    StructField('artist_longitude', DoubleType(), True),
    StructField('artist_name', StringType(), True),
    StructField('duration', DoubleType(), True),
    StructField('num_songs', LongType(), True),
    StructField('song_id', StringType(), True),
    StructField('title', StringType(), True),
    StructField('year', LongType(), True)
]))


log_schema = (StructType([
    StructField('artist', StringType(), True),
    StructField('auth', StringType(), True),
    StructField('firstName', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('itemInSession', LongType(), True),
    StructField('lastName', StringType(), True),
    StructField('length', DoubleType(), True),
    StructField('level', StringType(), True),
    StructField('location', StringType(), True),
    StructField('method', StringType(), True),
    StructField('page', StringType(), True),
    StructField('registration', DoubleType(), True),
    StructField('sessionId', LongType(), True),
    StructField('song', StringType(), True),
    StructField('status', LongType(), True),
    StructField('ts', LongType(), True),
    StructField('userAgent', StringType(), True),
    StructField('userId', StringType(), True)
]))
