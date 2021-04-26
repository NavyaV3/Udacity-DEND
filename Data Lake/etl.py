import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

start = datetime.now()
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY' ]=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create and run a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads data from JSON song files, loads it in song and artists tables and writes them as parquet files in the output folder
    
    parameters:
        spark : spark session
        input_data : file path of input json files
        output_data : file path to write the parquet files to
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()
    
    # create view of songs data to extract using SQL queries
    df.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
                        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
                        FROM songs_data""")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet(output_data + "songs/",mode = 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""
                          SELECT DISTINCT artist_id,
                          artist_name AS name,
                          artist_location AS location,
                          artist_latitude AS latitude,
                          artist_longitude AS longitude
                          FROM songs_data""")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/",mode = 'overwrite')



def process_log_data(spark, input_data, output_data):
    """
    Reads data from JSON log files, loads users, time and songplays tables and writes them as parquet files in the output folder
    
    parameters:
        spark : spark session
        input_data : file path of input json files
        output_data : file path to write the parquet files to
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
        
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # create view of log data to extract using SQL queries
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql("""
                        SELECT DISTINCT userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender,
                        level
                        FROM log_data""")
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/",mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time",get_timestamp("ts"))
    
    # create view of log data to extract time data using SQL queries
    df.createOrReplaceTempView("log_time_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
                       SELECT start_time,
                       hour(start_time) as hour,
                       dayofmonth(start_time) as day,
                       weekofyear(start_time) as week,
                       month(start_time) as month,
                       year(start_time) as year,
                       dayofweek(start_time) as weekday
                       FROM log_time_data""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet(output_data + "time/",mode = 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")
    
    # create view of song_df for SQL queries
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                            SELECT row_number() over (order by ltd.start_time) as songplay_id,
                            ltd.start_time,
                            ltd.userId AS user_id,
                            ltd.level,
                            sd.song_id,
                            sd.artist_id,
                            ltd.sessionId AS session_id,
                            ltd.location,
                            ltd.userAgent AS user_agent,
                            year(ltd.start_time) AS year,
                            month(ltd.start_time) AS month
                            FROM log_time_data ltd
                            JOIN song_data sd
                            ON (ltd.song = sd.title AND ltd.artist = sd.artist_name)""")
    
    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(["year","month"]).parquet(output_data + "songplays/",mode = 'overwrite')


def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print("ETL pipeline completed")
    
    # get the total time taken to load the data
    end = datetime.now()
    duration = end-start
    seconds = duration.total_seconds()
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    print("Time taken: {} hours {} minutes {}s".format(hours,minutes,seconds))


if __name__ == "__main__":
    main()
