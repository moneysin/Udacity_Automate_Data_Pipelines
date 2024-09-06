class SqlQueries:

## DROP TABLES

    staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
    staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
    songplay_table_drop = "DROP TABLE IF EXISTS songplays"
    user_table_drop = "DROP TABLE IF EXISTS users"
    song_table_drop = "DROP TABLE IF EXISTS songs"
    artist_table_drop = "DROP TABLE IF EXISTS artists"
    time_table_drop = "DROP TABLE IF EXISTS time"

## CREATE TABLES
    
staging_songs_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
    song_id text,
    num_songs int NOT NULL,
    title varchar(300) NOT NULL,
    artist_name varchar(500),
    artist_latitude float,
    year int NOT NULL,
    duration float NOT NULL,
    artist_id text,
    artist_longitude float,
    artist_location varchar(500));
""")

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
    artist text,
    auth text NOT NULL,
    firstName varchar(100),
    gender text,
    itemInSession int NOT NULL,
    lastName varchar(100),
    length float,
    level text NOT NULL,
    location varchar,
    method text NOT NULL,
    page text NOT NULL,
    registration double precision,
    sessionId int NOT NULL,
    song text,
    status int NOT NULL,
    ts bigint NOT NULL,
    userAgent text,
    userId int)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
    songplay_id varchar(50) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL, 
    user_id int, 
    level text NOT NULL, 
    song_id text, 
    artist_id text, 
    sessionid int NOT NULL, 
    location text, 
    useragent text);
""")
    
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
    user_id int PRIMARY KEY,
    first_name varchar(50), 
    last_name varchar(100),
    gender text,
    level text NOT NULL)
""")
    
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
    song_id text PRIMARY KEY,
    title varchar(500) NOT NULL,
    artist_id text,
    year int NOT NULL,  
    duration float NOT NULL)
""")
    
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
    artist_id text PRIMARY KEY,
    artist_name varchar(500), 
    artist_location varchar(500),
    artist_latitude float,  
    artist_longitude float)
""")
    
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL, 
    hour int NOT NULL,  
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL,
    weekday int NOT NULL)
""")


## INSERT TABLES
    
songplay_table_insert = ("""
    INSERT INTO songplays(
        songplay_id,
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        sessionid, 
        location, 
        useragent    
    )
    SELECT
        md5(events.sessionid || events.start_time) AS songplay_id,
        start_time, 
        events.userId as user_id, 
        events.level, 
        songs.song_id, 
        songs.artist_id, 
        events.sessionId as sessionid, 
        events.location, 
        events.userAgent as useragent
    FROM (
        SELECT 
            TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
            *
        FROM 
            staging_events
        WHERE 
            page='NextSong'
    ) events
    LEFT JOIN 
        staging_songs songs
    ON 
        events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration;
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    SELECT distinct userId as user_id, 
        firstName as first_name, 
        lastName as last_name, 
        gender, 
        level
    FROM staging_events
    WHERE page='NextSong'
    """)

song_table_insert = ("""
    INSERT INTO songs(
        song_id, 
        title, 
        artist_id, 
        year, 
        duration)
    SELECT distinct song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs
    """)

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude)
    SELECT distinct artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs
    """)

time_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT start_time, 
        extract(hour from start_time), 
        extract(day from start_time), 
        extract(week from start_time), 
        extract(month from start_time), 
        extract(year from start_time), 
        extract(dayofweek from start_time)
    FROM songplays
    """)


