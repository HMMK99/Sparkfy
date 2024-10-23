# DROP TABLES

def create_query(table_name, attrs):
    columns = ' '.join([f"{k} {v}," for k,v in attrs.items()])[:-1]
    return f"CREATE TABLE IF NOT EXISTS {table_name} \
    ({columns})"

def drop_query(table_name):
    return f"DROP TABLE IF EXISTS {table_name}"

# songplays - records in log data associated with song plays i.e. records with page NextSong
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# users - users in the app
    # user_id, first_name, last_name, gender, level
# songs - songs in music database
    # song_id, title, artist_id, year, duration
# artists - artists in music database
    # artist_id, name, location, latitude, longitude
# time - timestamps of records in songplays broken down into specific units
    # start_time, hour, day, week, month, year, weekday

songplay_table_drop = drop_query('SONGPLAY')
user_table_drop = drop_query('USERS')
song_table_drop = drop_query('SONG')
artist_table_drop = drop_query('ARTIST')
time_table_drop = drop_query('TIME')

# CREATE TABLES

songplay_table_create = create_query('SONGPLAY', {'songplay_id': 'int', 'start_time': 'timestamp', 'user_id': 'varchar', 'level': 'varchar', 'song_id': 'varchar', 'artist_id': 'varchar', 'session_id': 'int', 'location': 'varchar', 'user_agent': 'varchar'})

user_table_create = create_query('USERS', {'user_id': 'int', 'first_name': 'varchar', 'last_name': 'varchar', 'gender': 'varchar', 'level': 'int'})

song_table_create = create_query('SONG', {'song_id': 'varchar', 'title': 'varchar', 'artist_id': 'varchar', 'year': 'int', 'duration': 'numeric'})

artist_table_create = create_query('ARTIST', {'artist_id': 'varchar', 'name': 'varchar', 'location': 'varchar', 'latitude': 'numeric', 'longitude': 'numeric'})

time_table_create = create_query('TIMES', {'start_time': 'varchar', 'hour': 'varchar', 'day': 'varchar', 'week': 'int', 'month': 'numeric', 'year': 'numeric', 'weekday': 'numeric'})

# INSERT RECORDS

songplay_table_insert = """INSERT INTO SONGPLAY
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES """

user_table_insert = """INSERT INTO USERS
(user_id, first_name, last_name, gender, level) VALUES """

song_table_insert = """INSERT INTO SONG
(song_id, title, artist_id, year, duration) VALUES """

artist_table_insert = """INSERT INTO ARTIST
(artist_id, name, location, latitude, longitude) VALUES """


time_table_insert = """INSERT INTO TIME
(start_time, hour, day, week, month, year, weekday) VALUES """

# FIND SONGS

song_select = """SELECT * FROM SONG
"""

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]