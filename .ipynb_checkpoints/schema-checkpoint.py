from pyspark.sql.types import *

def songSchema():
    return StructType(
        [
            StructField('artist_id', StringType(), True),
            StructField('artist_latitude', DecimalType(), True),
            StructField('artist_longitude', DecimalType(), True),
            StructField('artist_location', StringType(), True),
            StructField('artist_name', StringType(), True),
            StructField('duration', DecimalType(), True),
            StructField('num_songs', IntegerType(), True),
            StructField('song_id', StringType(), True),
            StructField('title', StringType(), True),
            StructField('year', IntegerType(), True)
        ]
    )


def logSchema():
    return StructType(
        [
            StructField('artist', StringType(), True),
            StructField('auth', StringType(), True),
            StructField('firstName', StringType(), True),
            StructField('gender', StringType(), True),
            StructField('itemInSession', IntegerType(), True),
            StructField('lastName', StringType(), True),
            StructField('length', DecimalType(), True),
            StructField('level', StringType(), True),
            StructField('location', StringType(), True),
            StructField('method', StringType(), True),
            StructField('page', StringType(), True),
            StructField('registration', LongType(), True),
            StructField('sessionId', IntegerType(), True),
            StructField('song', StringType(), True),
            StructField('status', IntegerType(), True),
            StructField('ts', LongType(), True),
            StructField('userAgent', StringType(), True),
            StructField('userId', StringType(), True)
        ]
    )
