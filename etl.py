import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_date, to_timestamp, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create Spark Session"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Load song data json files from S3, process in Spark to extract songs_table and artists_table, 
       and write the tables back to S3 as parquet files"""
    
    # get filepath to song data file, I only read the data from A/A/ folder because reading the whole data set is slow
    song_data = input_data + 'song_data/A/A/*'
    
    # read song data file
    df = spark.read.format("json").load(song_data)
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT song_id, title, artist_id, year,duration  
    FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs-table")

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
    FROM songs
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists_table")


def process_log_data(spark, input_data, output_data):
    """Load log data json files from S3, process in Spark to extract users, time, and songplays tables, 
       and write the tables back to S3 as parquet files"""
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*'

    # read log data file
    df_log = spark.read.format("json").load(log_data)
    df_log.createOrReplaceTempView("logs")
    
    # filter by actions for song plays
    logs = spark.sql("""
    SELECT *
    FROM logs
    WHERE page = 'NextSong'
    """)

    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT userId,firstName,lastName,gender,level 
    FROM logs
    """)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"users_table")

    # create timestamp column from original timestamp column
    logs = logs.withColumn("start_time_string", from_unixtime((logs.ts / 1000)))
    logs = logs.withColumn("start_time", (col("start_time_string").cast("timestamp")))
    
    # extract columns to create time table
    time_table = logs.select('start_time',hour(col('start_time')).alias("hour"),dayofmonth(col('start_time')).alias("day"), 
                             weekofyear(col('start_time')).alias("week"), 
                             month(col('start_time')).alias("month"),year(col('start_time')).alias("year"), 
                             dayofweek(col('start_time')).alias("weekday"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"time_table")

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/A/A/*'
    song_df = spark.read.format("json").load(song_data)
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    logs.createOrReplaceTempView("logs_time")
    songplay_table = spark.sql("""
             SELECT  monotonically_increasing_id() as songplay_id, start_time, userId as user_id, level, song_id, artist_id,sessionId as session_id, location, userAgent as user_agent, year(start_time) as year, month(start_time) as month                    
             FROM logs_time, songs
             WHERE title = song AND artist_name = artist
              """) 

    # subset the song_play table since we don't need year and month in the output but need to partition by the "year" and "month"
    df_songplay = songplay_table.toPandas()
    years = list(set(df_songplay['year'].tolist()))
    months = list(set(df_songplay['month'].tolist()))
    songplay_filter=df_songplay[['songplay_id','start_time','user_id','level','song_id',
                                   'artist_id','session_id','location','user_agent']]
    
    # write songplays table to parquet files partitioned by year and month
    for y in years:
        for m in months:
            #subset DF
            df_to_parquet = songplay_filter.loc[(df_songplay['year']==int(y)) & (df_songplay['month']==int(m))]
            to_parquet = spark.createDataFrame(df_to_parquet)
            to_parquet.write.mode("overwrite").parquet("{}songplays_table/{}/{}/songsplays_table.parquet".format(output_data,y,m))


def main():
    """Main function to create spark session and start ETL process"""
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
