# Data-Lake-Spark-S3
<h2>Introduction</h2>
The music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake on AWS. <strong>This project builds an ETL pipeline that extracts their data in json files from S3, processes data using Spark, and loads processed data back into S3 as a set of tables stored in parquet files for the use of the analytics team </strong>

-------------------------------------------------

<h2>Star Schema Designed Tables(Fact and Dimension Tables)</h2>
<h4>Fact Table:</h4>
<ol>
<li>songplay_table - records in event data associated with song plays i.e. records with page NextSong</li>
<ul>
<li>songplay_id (primarykey), start_time(reference to time_table(start_time)), user_id(reference to user_table(user_id)), level, song_id(reference to song_table(song_id)), artist_id(reference to artist_table(artist_id)), session_id, location, user_agent</li>
</ul>
</ol>

<h4>Dimension Tables:</h4>
<ol>
<li>user_table - users in the app</li>
<ul>
<li>user_id(primarykey), first_name, last_name, gender, level</li>
</ul>
<li>song_table - songs in music database</li>
<ul>
<li>song_id(primarykey), title, artist_id, year, duration</li>
</ul>
<li>artist_table - artists in music database</li>
<ul>
<li>artist_id(primarykey), name, location, latitude, longitude</li>
</ul>
<li>time_table - timestamps of records in songplays broken down into specific units</li>
<ul>
<li>start_time(primarykey), hour, day, week, month, year, weekday</li>
</ul>
</ol>

---------------------------------

<h2>ETL Pipeline</h2>
etl.py reads the songs and log json files from the S3, processes the files with Spark to extract the tables mentioned above, and writes the tables back to S3 as a set of partitioned parquet files<br>
The dl.cfg contains the credentials for accessing AWS. 

---------------------------------

<h2>Running Instruction</h2>
<ol>
<li>Run etl.py to do the whole ETL process</li>
</ol>
NOTE: Credentials for AWS have been removed from the dl.cfg. Remember to fill your own credentials in order to run the etl.py file)

