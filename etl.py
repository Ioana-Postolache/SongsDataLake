import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long, TimestampType as Ts


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
        Description: This function can be used to create a spark session.
        Arguments:
            None
        Returns:
            SparkSession
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
        Description: This function can be used to load the song data from the input S3 bucket
                     and write the parquet files to the output S3 bucket.
        Arguments:
            spark: SparkSession
            input_data: location for the input data
            output_data: location for the output data
        Returns:
            None
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    print(song_data)

    # read song data file
    songsSchema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",Str()),
    Fld("duration",Dbl()),
    Fld("num_songs",Int()),
    Fld("song_id",Str()),    
    Fld("title",Str()),    
    Fld("year",Int())    
])

    df = spark.read.json(song_data, schema=songsSchema).distinct()
    print(df.count())
    print(df.show(5, truncate=False))

    df.printSchema()

    # extract columns to create songs table
    
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    songs_table.printSchema()
    songs_table.show(5)
    print('songs', songs_table.count())
    
     # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    df.createOrReplaceTempView("df")
    artists_table = spark.sql("select artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude from df").distinct()
    artists_table.printSchema()
    artists_table.show(5)
    print('artists', artists_table.count())

    # write artists table to parquet files
    artists_table.repartitionByRange(3, "artist_id").write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    '''
        Description: This function can be used to load the log data from the input S3 bucket
                     and write the parquet files to the output S3 bucket.
        Arguments:
            spark: SparkSession
            input_data: location for the input data
            output_data: location for the output data
        Returns:
            None
    '''
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")
    print(log_data)    
   
    logsSchema = R([
    Fld("artist",Str()),
    Fld("auth",Str()),
    Fld("firstName",Str()),
    Fld("gender",Str()),
    Fld("itemInSession",Int()),
    Fld("lastName",Str()),
    Fld("length",Dbl()),
    Fld("level",Str()),
    Fld("location",Str()),
    Fld("method",Str()),
    Fld("page",Str()),    
    Fld("registration",Dbl()),    
    Fld("sessionId",Long()),
    Fld("song",Str()),
    Fld("status",Int()),
    Fld("ts",Long()),
    Fld("userAgent",Str()),
    Fld("userId",Str()) 
])

    # read log data file
    df = spark.read.json(log_data, schema=logsSchema).distinct()
    #df = spark.read.json(log_data)
    print('df.count', df.count())
    print(df.show(5, truncate=False))
    df.printSchema()
    
    # filter by actions for song plays
    dfSongPlays = df.filter("page == 'NextSong'")
    
    # extract columns for users table  
    dfSongPlays.createOrReplaceTempView("dfSongPlays")
    users_table = spark.sql("select userId as user_id, firstName as first_name, lastName as last_name, gender, level from dfSongPlays").distinct()
    print('users_table.count', users_table.count())
    # write users table to parquet files
    users_table.repartitionByRange(3, "user_id").write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    dfWithDatetime = dfSongPlays.withColumn('datetime', from_unixtime(dfSongPlays.ts/1000))
    print('after adding datetime')
    dfWithDatetime.show(5, truncate=False)
    # extract columns to create time table
    dfWithDatetime.createOrReplaceTempView("dfWithDatetime")
    time_table = spark.sql("""
                                select 
                                        ts as start_time, 
                                        hour(datetime) as hour, 
                                        dayofmonth(datetime) as day, 
                                        weekofyear(datetime) as week, 
                                        month(datetime) as month, 
                                        year(datetime) as year, 
                                        dayofweek(datetime) as weekday 
                                    from dfWithDatetime
                                """).distinct()
    time_table.show(5, truncate=False)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table    
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    # read song data file
    songsSchema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",Str()),
    Fld("duration",Dbl()),
    Fld("num_songs",Int()),
    Fld("song_id",Str()),    
    Fld("title",Str()),    
    Fld("year",Int())    
])
    song_df = spark.read.json(song_data, schema=songsSchema).distinct()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = dfSongPlays.join(song_df, (dfSongPlays.artist == song_df.artist_name) & (dfSongPlays.song == song_df.title), how='left') \
                    .withColumn("songplay_id", monotonically_increasing_id()) \
                    .withColumn("year", year(from_unixtime(dfSongPlays.ts/1000))) \
                    .withColumn("month", month(from_unixtime(dfSongPlays.ts/1000))) # Could also use 'left_outer'
    songplays_df.show(5)
    songplays_df.createOrReplaceTempView("songplays_df")
    songplays_table = spark.sql("""
                                    select 
                                            songplay_id, 
                                            ts as start_time, 
                                            userId as user_id, 
                                            level,
                                            song_id, 
                                            artist_id, 
                                            sessionId as session_id, 
                                            location, 
                                            userAgent as user_agent,
                                            year, 
                                            month
                                        from songplays_df
                                """).distinct()
     
    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
#     input_data = ""
    output_data = "s3a://songs-data-lake/"
#     output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
