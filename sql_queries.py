# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 2020

@author: gari.ciodaro.guerra

Set of functions to create queries for, create, drop,
insert tables on Amazon redshift.
"""

import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Load configuration parameters
IAM_ROLE      = config.get("IAM_ROLE", "ARN")
LOG_DATA      = config.get("S3", "LOG_DATA")
SONG_DATA     = config.get("S3", "SONG_DATA")
LOG_JSONPATH  = config.get("S3", "LOG_JSONPATH")

# Auxiliar funtions
def create_tables(name,string_columns,distribution_style='auto'):
    """Returns string used to produce  CREATE statement.
    
    Parameters
    ----------
    name : string
        indicates the name of the table to create.
    string_columns : string
        list of columns to create.
    distribution_style : string
        Set the storage mode on Redshif:
        -auto : Let Redshif decide.
        -even : Storage data evenly on cpus.
        -all  : Storage all the table in all cpu.

    Returns
    -------
    query : string
    """
    create_string="CREATE TABLE IF NOT EXISTS "
    query=(create_string+
           name+
           string_columns+" diststyle "+distribution_style)
    return query

def drop_tables(name):
    """Returns string used to produce DROP statement.
    
    Parameters
    ----------
    name : string
        indicates the name of the table to delete.
        
    Returns
    -------
    query : string
    """
    drop_string="DROP TABLE IF EXISTS "
    query=drop_string+name
    return query

def insert_on_table(name,select_q):
    """Returns string used to produce insert statement.
    
    Parameters
    ----------
    name : string
        indicates the name of the table to do the insert.
    select_q : sring
        select query that produces origin data of the 
        insert statement

    Returns
    -------
    query : string
    """
    parameters=tables_dictionary.get(name).replace("(","").replace(")","").replace("0,1","")
    cols=[each.split(" ")[0] for each in parameters.split(",")]
    # Since songplay_id is an IDENTITY field
    # it should be remove from the query.
    if name=="songplays":
        cols.remove('songplay_id')
    query="INSERT INTO "+name+" ("+",".join(cols)+") "
    query=query+select_q
    return query

# Create tables dictionary. key is the name of the table.
# value is the columns.
tables_dictionary={
    "stagingevents":("(artist  TEXT ,"+
                "auth TEXT,"+
                "firstName  TEXT,"+
                "gender  VARCHAR(1) ,"+
                "itemInSession  INT,"+
                "lastName  TEXT,"+
                "length  NUMERIC,"+
                "level  VARCHAR(4),"+
                "location  TEXT,"+
                "method  VARCHAR(3),"+
                "page  TEXT,"+
                "registration  BIGINT,"+
                "sessionId  INT,"+
                "song  TEXT,"+
                "status  INT,"+
                "ts  BIGINT SORTKEY,"+
                "userAgent  TEXT,"+
                "userId INT)"),
    "stagingsongs":("(artist_id  TEXT ,"+
                "artist_latitude  NUMERIC,"+
                "artist_location  TEXT,"+
                "artist_longitude  NUMERIC,"+
                "artist_name  TEXT,"+
                "duration  NUMERIC,"+
                "num_songs  INT,"+
                "song_id  varchar(18) ,"+
                "title  TEXT,"+
                "year INT SORTKEY)"),
    "songplays":("(songplay_id INT IDENTITY(0,1) PRIMARY KEY,"+ 
                "start_time BIGINT NOT NULL SORTKEY,"+
                "user_id INT NOT NULL,"+ 
                "level varchar(4) NOT NULL,"+
                "song_id VARCHAR(18),"+
                "artist_id VARCHAR(18),"+ 
                "session_id INT NOT NULL," +
                "location TEXT NOT NULL," +
                "user_agent TEXT NOT NULL)"),
    "users":("(user_id INT PRIMARY KEY,"+ 
                "first_name TEXT NOT NULL,"+
                "last_name TEXT NOT NULL SORTKEY,"+ 
                "gender VARCHAR(1) NOT NULL,"+
                "level VARCHAR(4) NOT NULL)"),
    "songs":("(song_id VARCHAR(18) PRIMARY KEY,"+ 
                "title TEXT NOT NULL,"+
                "artist_id VARCHAR(18) NOT NULL,"+ 
                "year INT NOT NULL,"+
                "duration NUMERIC NOT NULL)"),
    "artists":("(artist_id varchar(18) PRIMARY KEY,"+ 
                "name TEXT NOT NULL,"+
                "location TEXT,"+ 
                "latitude NUMERIC,"+
                "longitude NUMERIC)"),
    "time":("(start_time BIGINT PRIMARY KEY,"+ 
                "hour INT NOT NULL,"+
                "day INT NOT NULL,"+ 
                "week INT NOT NULL,"+
                "month INT NOT NULL,"+
                "year INT NOT NULL,"+
                "weekday INT NOT NULL)"),
}

# Drop tables queries
staging_events_table_drop = drop_tables("stagingevents")
staging_songs_table_drop  = drop_tables("stagingsongs")
songplay_table_drop       = drop_tables("songplays")
user_table_drop           = drop_tables("users")
song_table_drop           = drop_tables("songs")
artist_table_drop         = drop_tables("artists")
time_table_drop           = drop_tables("time")

# Create tables queries.
staging_events_table_create= create_tables("stagingevents",
                                    tables_dictionary.get("stagingevents"),
                                    "auto")

staging_songs_table_create = create_tables("stagingsongs",
                                    tables_dictionary.get("stagingsongs"),
                                    "auto")

songplay_table_create = create_tables("songplays",
                                    tables_dictionary.get("songplays"),
                                    "even")

user_table_create = create_tables("users",
                                    tables_dictionary.get("users"),
                                    "all")

song_table_create = create_tables("songs",
                                    tables_dictionary.get("songs"),
                                    "all")

artist_table_create =create_tables("artists",
                                    tables_dictionary.get("artists"),
                                    "all")

time_table_create = create_tables("time",tables_dictionary.get("time"),"auto")

# Staging queries allow us to bring
# data from the json files in S3
# to the staging area of our Data warehouse.

# Location of the S3 files.
LOG_DATA      = config.get("S3", "LOG_DATA")
SONG_DATA     = config.get("S3", "SONG_DATA")
LOG_JSONPATH  = config.get("S3", "LOG_JSONPATH")

# The json command on LOG_JSONPATH allow us to
# give tempory structure to the files in
# LOG_DATA folders. 
staging_events_copy = ("""
    COPY stagingevents FROM '{}'
    format as json '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2';
""").format(LOG_DATA,LOG_JSONPATH,IAM_ROLE)

# loading song meta data.
staging_songs_copy = ("""
    COPY stagingsongs FROM '{}'
    format as json 'auto'
    credentials 'aws_iam_role={}'
    region 'us-west-2';
""").format(SONG_DATA,IAM_ROLE)

# insert statements from staging tables.
# not insert with null primary key is allowed.
user_table_insert  = insert_on_table("users",
                    select_q="SELECT DISTINCT userId,firstName,lastName,\
                    gender,level FROM stagingevents WHERE userId IS NOT NULL")

song_table_insert  = insert_on_table("songs",
                                select_q="SELECT DISTINCT song_id, title, \
                                artist_id, year, duration FROM stagingsongs WHERE song_id IS NOT NULL")

artist_table_insert= insert_on_table("artists",
                    select_q="SELECT DISTINCT artist_id,artist_name,artist_location,\
                    artist_latitude,artist_longitude FROM stagingsongs WHERE artist_id IS NOT NULL")

# this query transform timestamp of the events
# into desired estructere of the table time.
# filter only on page = 'NextSong'
get_times="""
    select
        a.ts,
        extract(hour from a.human_time) as hour,
        extract(day from a.human_time) as day,
        extract(week from a.human_time) as week,
        extract(month from a.human_time) as month,
        extract(year from a.human_time) as year,
        extract(weekday from a.human_time) as weekday
    from (
        select ts,
            timestamp 'epoch' + ts / 1000 * interval '1 second' as human_time
        FROM stagingevents
        WHERE page = 'NextSong' AND ts IS NOT NULL
    ) a
"""
time_table_insert =insert_on_table("time",select_q=get_times)

# Inser information on the fact table.
insert_query_songplays="""
    SELECT e.ts,e.userId,e.level,s.song_id,s.artist_id,e.sessionId,e.location,
        e.userAgent  
    FROM  
    (SELECT ts, userId, level, sessionId, location, userAgent,artist,length,
        song FROM stagingevents WHERE userId IS NOT NULL AND page = 'NextSong') e 
     JOIN  
    (SELECT a.song_id, a.artist_id, a.duration, b.name, a.title FROM 
        songs a JOIN artists b on a.artist_id=b.artist_id) s 
    ON e.length=s.duration  AND e.artist=s.name AND e.song=s.title;
    """

songplay_table_insert = insert_on_table("songplays",select_q=insert_query_songplays)


# Store all the string queries into lists to import to other scripts.
create_table_queries = [staging_events_table_create,staging_songs_table_create, 
                        songplay_table_create, user_table_create, 
                        song_table_create, artist_table_create, 
                        time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                        songplay_table_drop, user_table_drop, song_table_drop, 
                        artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [user_table_insert, 
                        song_table_insert, artist_table_insert, 
                        time_table_insert,songplay_table_insert]
