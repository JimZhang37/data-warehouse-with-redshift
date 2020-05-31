import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA        = config.get('S3', 'LOG_DATA') 
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
ARN             = config.get('IAM_ROLE','ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE if exists staging_events"
staging_songs_table_drop = "DROP TABLE if exists staging_songs"
songplay_table_drop = "DROP TABLE if exists songplays"
user_table_drop = "DROP TABLE if exists users"
song_table_drop = "DROP TABLE if exists songs"
artist_table_drop = "DROP TABLE if exists artists"
time_table_drop = "DROP TABLE if exists time"

# CREATE TABLES

staging_songs_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs text,
    artist_id text,
    artist_latitude text,
    artist_longitude text,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration text,
    year text
)
""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession text,
    lastName text,
    length text,
    level text,
    location text,
    method text,
    page text,
    registration text,
    sessionId text,
    song text,
    status text,
    ts bigint,
    userAgent text,
    userId text
)
""")

# serial?
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp, 
    user_id varchar, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id integer, 
    location varchar, 
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
);

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year integer, 
    duration numeric
);

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude varchar, 
    longitude varchar
);

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour integer, 
    day integer, 
    week integer, 
    month integer, 
    year integer, 
    weekday integer
);

""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from {}
credentials 'aws_iam_role={}'
FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
copy staging_songs
from {}
credentials 'aws_iam_role={}'
json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT   TIMESTAMP 'epoch' + events.ts/1000 *INTERVAL '1 second' AS start_time,
         events.userId AS user_id,
         events.level,
         songs.song_id,
         songs.artist_id,
         events.sessionId ::integer AS session_id,
         events.location,
         events.userAgent AS user_agent
FROM staging_events AS events
JOIN staging_songs AS songs
ON (events.artist = songs.artist_name)
     AND (events.song = songs.title)
     AND (events.length = songs.duration)
WHERE events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id , first_name , last_name , gender , level)
SELECT  userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
        
FROM staging_events;
""")

song_table_insert = ("""
INSERT INTO songs (song_id , title , artist_id , year , duration)
SELECT  song_id,
        title,
        artist_id,
        year::integer,
        duration::numeric
        
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id , name , location , latitude , longitude)
SELECT  artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longtitude
        
        
FROM staging_songs;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  a.start_time,
        EXTRACT (HOUR FROM a.start_time), 
        EXTRACT (DAY FROM a.start_time),
        EXTRACT (WEEK FROM a.start_time), 
        EXTRACT (MONTH FROM a.start_time),
        EXTRACT (YEAR FROM a.start_time), 
        EXTRACT (WEEKDAY FROM a.start_time) 
FROM (  SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time 
        FROM staging_events) a;

""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
