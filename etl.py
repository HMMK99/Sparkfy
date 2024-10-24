import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
    # open song file
    filepath_song = get_files(filepath)

    for file in filepath_song:
        try:
            df1 = pd.read_json(file, lines=True)
            df = pd.concat([df, df1], ignore_index=True, sort=False)
        except Exception as e:
            df = pd.read_json(file, lines=True)

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.replace("'", "''")

    # insert song record
    song_data = list(f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}');" for x in df[['song_id', 'title', 'artist_id', 'year', 'duration']].values)
    for data in song_data:
        cur.execute(song_table_insert + data)
    
    # insert artist record
    artist_data = list(f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}');" for x in df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values)
    for data in artist_data:
        cur.execute(artist_table_insert + data)

def song_select(song, artist, duration):
    return f"SELECT artist_id, song_id FROM SONG JOIN ARTIST USING(ARTIST_ID) WHERE TITLE = '{song}'    AND NAME = '{artist}'    AND duration = {duration}"

def process_log_file(cur, filepath):
    # open log file
    filepath_log = get_files(filepath)

    for file in filepath_log:
        try:
            df1 = pd.read_json(file, lines=True)
            df = pd.concat([df, df1], ignore_index=True, sort=False)
        except Exception as e:
            df = pd.read_json(file, lines=True)

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.replace("'", "''")

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = df.copy()
    t['ts'] = pd.to_timedelta(t['ts'], unit='ms')

    # Set a base datetime (e.g., 1970-01-01)
    base_datetime = pd.Timestamp('1970-01-01')

    # Add the timedelta to the base datetime
    t['ts'] = base_datetime + t['ts']

    t['timestamp'] = t['ts'].dt  # Full timestamp
    t['hour'] = t['ts'].dt.hour.tolist()  # Hour
    t['day'] = t['ts'].dt.day.tolist()  # Day
    t['weekOmonth'] = t['ts'].dt.isocalendar().week.tolist()  # Week of year (ISO calendar)
    t['month'] = t['ts'].dt.month.tolist()  # Month
    t['year'] = t['ts'].dt.year.tolist()  # Year
    t['weekday'] = t['ts'].dt.weekday.tolist() 
    
    # insert time data records
    # time_data = 
    # column_labels = 
    time_df = t[t.columns[-7:]]

    times = list(f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}', '{x[5]}', '{x[6]}');" for x in time_df.values)

    for timo in times:
        cur.execute(time_table_insert + timo)

    # load user table
    df_user = t.dropna()
    user_df = df_user[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    usersy = list(f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}');" for x in user_df.values)

    for usery in usersy:
        cur.execute(user_table_insert + usery)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        cur.execute('SELECT artist_id, song_id, TITLE, NAME, duration FROM SONG JOIN ARTIST USING(ARTIST_ID) ')
        artSongData = cur.fetchall()
        artSongDataDf = pd.DataFrame(columns=['artist_id', 'song_id', 'TITLE', 'NAME', 'duration'], data=artSongData)
        x = 341000
        y = 31
        for row in range(t.shape[0]):

            try:
                songid = artSongDataDf[(artSongDataDf.TITLE == t.loc[row, 'song']) 
                                        &  (artSongDataDf.NAME == t.loc[row, 'artist'])  
                                        & (artSongDataDf.duration == float(t.loc[row, 'length']))
                                        ].iloc[0,0]
                artistid = artSongDataDf[(artSongDataDf.TITLE == t.loc[row, 'song']) 
                                        &  (artSongDataDf.NAME == t.loc[row, 'artist'])  
                                        & (artSongDataDf.duration == float(t.loc[row, 'length']))
                                        ].iloc[0,1]
                print(songid, artistid, end='\n')
            except Exception as e:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = f'({x}, \'{t.loc[row, "ts"]}\', \'{t.loc[row, "userId"]}\', \'{t.loc[row, "level"]}\', \'{songid}\', \'{artistid}\', \'{t.loc[row, "sessionId"]}\', \'{t.loc[row, "location"]}\', \'{t.loc[row, "userAgent"]}\');'
            x += y
            y += 1
            print(y, end=', ')
            cur.execute(songplay_table_insert + songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
