import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    event_id INT IDENTITY(0,1) NOT NULL SORTKEY DISTKEY ,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT NOT NULL,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR PRIMARY KEY,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INTEGER PRIMARY KEY,
first_name      VARCHAR NOT NULL,
last_name       VARCHAR NOT NULL,
gender          VARCHAR NOT NULL,
level           VARCHAR NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id          VARCHAR PRIMARY KEY,
name               VARCHAR NOT NULL,
location           VARCHAR,
latitude           FLOAT,
longitude          FLOAT
)
""")


song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR NOT NULL,
artist_id   VARCHAR NOT NULL REFERENCES artists(artist_id),
year        INTEGER NOT NULL,
duration    FLOAT NOT NULL
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time    TIMESTAMP PRIMARY KEY,
hour          INTEGER NOT NULL,
day           INTEGER NOT NULL,
week          INTEGER NOT NULL,
month         INTEGER NOT NULL,
year          INTEGER NOT NULL,
weekday       VARCHAR NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
start_time           TIMESTAMP NOT NULL REFERENCES time(start_time),
user_id              INTEGER NOT NULL REFERENCES users(user_id),
level                VARCHAR NOT NULL,
song_id              VARCHAR NOT NULL REFERENCES songs(song_id),
artist_id            VARCHAR NOT NULL REFERENCES artists(artist_id),
session_id           INTEGER NOT NULL,
location             VARCHAR NOT NULL,
user_agent           VARCHAR NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
                start_time,
                user_id,
                level,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent)
         SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.userId AS user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId AS session_id,
                se.location,
                se.userAgent AS user_agent
           FROM staging_events se
           JOIN staging_songs ss
             ON se.artist = ss.artist_name
            AND se.song = ss.title
          WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id, 
    artist_name as name, 
    artist_location as location, 
    artist_latitude as latitude, 
    artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time AS start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEKS FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    to_char(start_time, 'Day') AS weekday
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
