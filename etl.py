import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # schema of the data
    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    song_fields = ["title", "artist_id","year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/', mode="overwrite")

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", 
                      "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
     
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time")) \
                   .withColumn("day",dayofmonth("start_time")) \
                   .withColumn("week",weekofyear("start_time")) \
                   .withColumn("month",month("start_time")) \
                   .withColumn("year",year("start_time")) \
                   .withColumn("weekday",dayofweek("start_time")) \
                   .select("ts","start_time","hour", "day", "week", "month", "year", "weekday") \
                   .drop_duplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time_table/', mode="overwrite")

    # read in song data to use for songplays table
#     song_df = spark.read\
#                 .format("parquet")\
#                 .option("basePath", os.path.join(output_data, "songs/"))\
#                 .load(os.path.join(output_data, "songs/*/*/"))
    song_df = spark.read.parquet(output_data + 'songs/*/*/*')
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title)
    
    songplays_table = songplays_table.select(monotonically_increasing_id().alias("songplay_id"),
                               col("start_time"), \
                               col("userId").alias("user_id"),"level","song_id","artist_id",  
                               col("sessionId").alias("session_id"), "location", \
                               col("userAgent").alias("user_agent"))
 
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time) \
                                     .select("songplay_id", "start_time", "user_id", "level", "song_id", \
                                             "artist_id", "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/', mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-data-lake/processed/"    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
