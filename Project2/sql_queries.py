import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR, 
    item_in_session INT,
    last_name VARCHAR,
    length FLOAT,
    level IDENTITY(free, paid),
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INT,
    song VARCHAR,
    status INT,
    ts INT,
    user_agent VARCHAR,
    user_id INT);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs 
    (
    num_songs INT,
    artist_id INT,
    artist_latitude INT,
    artist_longitude INT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id INT,
    title VARCHAR,
    duration FLOAT,
    year INT,
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (songplay_id int PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR,
    song_id INT,
    artist_id INT,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
    song_id INT PRIMARY KEY,
    title VARCHAR,
    artist_id INT,
    duration FLOAT);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
    artist_id INT PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    lattitude INT,
    longitude INT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day VARCHAR,
    week VARCHAR,
    month VARCHAR,
    year INT,
    weekday VARCHAR)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json {};
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH")
           )

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto'
""").format(
            config.get("S3", "SONG_DATA"),
            config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + (se.ts/1000*INTERVAL '1 second'),
        se.user_id,
        se.level,
        so.song_id,
        so.artist_id,
        se.location,
        se.user_agent
        FROM staging_events as se
        LEFT JOIN staging_songs as so ON se.song=so.title 
            AND se.artist = so.artist_name 
            AND ABS(se.length - so.duration) < 2
            WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT(user_id)
    user_id,
    first_name,
    last_name,
    gender,
    level
    FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT(song_id)
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT (artist_id)
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time
    WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') AS ts
    FROM staging_events)
    SELECT DISTINCT
    ts,
    extract(hour from ts),
    extract(day from ts),
    extract(week from ts),
    extract(year from ts),
    extract(weekday from ts)
    FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
