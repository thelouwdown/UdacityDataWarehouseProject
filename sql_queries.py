import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# The Staging tables
# Here you dump all variables from the initial files regardless of if you use them or not

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR,
auth VARCHAR,
first_name VARCHAR,
gender VARCHAR(1),
item_in_session VARCHAR,
last_name VARCHAR,
length FLOAT NULL,
level VARCHAR,
location VARCHAR,
method VARCHAR(10),
page VARCHAR,
registration FLOAT,
session_id INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
user_agent VARCHAR,
user_id INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
artist_id VARCHAR,
artist_latitude VARCHAR,
artist_location VARCHAR,
artist_longitude VARCHAR,
artist_name VARCHAR,
duration FLOAT,
num_songs INT,
song_id VARCHAR(100),
title VARCHAR,
year INT
);
""")

#songplay is the fact table
#INTEGER IDENTITY(1,1) creates the 'index' for the table. The first 1 means the starting value of ID and the second 1 means the increment value of ID.
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY, 
start_time TIMESTAMP DISTKEY SORTKEY, 
user_id INT, 
level VARCHAR, 
song_id VARCHAR(100), 
artist_id VARCHAR(100), 
session_id INT, 
location VARCHAR,
user_agent VARCHAR
);
""")


#users, songs, artists and times are the fact tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id int PRIMARY KEY SORTKEY, 
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR(1),
level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id VARCHAR(100) PRIMARY KEY SORTKEY,
title VARCHAR ,
artist_id VARCHAR ,
year INT ,
duration FLOAT
);
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
artist_id VARCHAR PRIMARY KEY SORTKEY, 
name VARCHAR,
location VARCHAR,
latitude VARCHAR,
longitude VARCHAR
);
""")


time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
start_time TIMESTAMP PRIMARY KEY DISTKEY SORTKEY, 
hour INT, 
day INT, 
week INT, 
month INT, 
year INT, 
weekday VARCHAR
);
""")


# STAGING TABLES

#note here that we use the LOG_JSONPATH for staging the events data as the variable names we specified in the table are #different to that in the columns. i.e. we use last_name instead of lastName. So for the Songs data we dont need to add the #JSON URL path because that JSON file's columns are named correctly e.g last_name = last_name (for example but there is no #last_name column in songs- its just illustrative)



staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
format as json {}; 
""").format(LOG_DATA,IAM,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
json 'auto'
""").format(SONG_DATA, IAM)



# FINAL TABLES

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT user_id, first_name,last_name,gender,level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong'

""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id ,title,artist_id,year,duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time,
se.user_id,
se.level,
ss.song_id,
ss.artist_id,
se.session_id,
se.location,
se.user_agent
FROM staging_events se
JOIN staging_songs ss ON ( se.song = ss.title and se.artist = ss.artist_name)
WHERE se.page = 'NextSong'

""")


artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE song_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day , week, month, year, weekday)
SELECT DISTINCT start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM (
SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
FROM staging_events
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
