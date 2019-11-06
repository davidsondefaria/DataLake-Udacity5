import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import types as T
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date, dayofweek

from pyspark.sql.functions import to_timestamp, from_unixtime

from schema import songSchema
from schema import logSchema


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # song filepath
    song_path = 'song_data/A/A/A/*.json'
    
    # get filepath to song data file
    song_data = input_data + song_path
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema())
    
    print("\n# Songs #####################################\n")
    df.printSchema()
    df.show(5)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table.show(5)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name')
    artists_table.show(5)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    # log filepath
    log_path = 'log_data/*/*/*.json'
    
    # get filepath to log data file
    log_data = os.path.join(input_data, log_path)

    # read log data file
    df = spark.read.json(log_data)#, schema=logSchema())
    
    print("\n# Logs #####################################\n")
    df.printSchema()
    df.show(5)
    
    # filter by actions for song plays
    df = df[df.page == 'NextSong']
    
    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level', 'userAgent')
    users_table.show(5)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", to_timestamp(from_unixtime((df.ts / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp"))
    df.show(5)
    
    # create datetime column from original timestamp column
    # https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2458071/Date+Functions+and+Properties+Spark+SQL
    df = df.withColumn("start_time", (df.timestamp).cast('Timestamp'))\
            .withColumn("hour", hour(df.timestamp).cast("Integer"))\
            .withColumn("day", dayofmonth(df.timestamp).cast("Integer"))\
            .withColumn("week", weekofyear(df.timestamp).cast("Integer"))\
            .withColumn("month", month(df.timestamp).cast("Integer"))\
            .withColumn("year", year(df.timestamp).cast("Integer"))\
            .withColumn("weekday", dayofweek(df.timestamp).cast("Integer"))
    df.show(5)
    
    # extract columns to create time table
    time_table = df.select('sessionId', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    songParquet = spark.read.parquet(output_data + "songs")
    songParquet.createOrReplaceTempView("songParquet")
    
    artistParquet = spark.read.parquet(output_data + "artists")
    artistParquet.createOrReplaceTempView("artistParquet")
    
    usersParquet = spark.read.parquet(output_data + "users")
    usersParquet.createOrReplaceTempView("usersParquet")
    
    timeParquet = spark.read.parquet(output_data + "time")
    timeParquet.createOrReplaceTempView("timeParquet")

    song_df = df.select('start_time', 'userId', 'level', 'sessionId', 'userAgent')
    
    song_sql = spark.sql("\
        SELECT sg.song_id, at.artist_id, at.artist_location\
        FROM (SELECT song_id, title, artist_id FROM songParquet) AS sg\
            JOIN (SELECT artist_id, artist_location FROM artistParquet) AS at\
            ON sg.artist_id = at.artist_id\
        WHERE sg.title = %s\
    "%(df['song']))
    
    song_sql

    songs = song_df.join(song_sql)
    
    songs.show()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songs.select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'artist_location', 'sessionId', 'userAgent')
    song_df.show()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data + "songplay")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifyout/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
