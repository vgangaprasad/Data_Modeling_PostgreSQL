import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Summary line
    ------------
    Process song files one by one and inserts the data to song and artists table.
    
    Parameters
    ----------
    cur -- Database Cursor
    filepath -- JSON file path which has the songs data
    
    Returns
    -------
    None
    
    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_cols = [7,8,0,9,5]
    song_data =  df[df.columns[song_cols]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_cols = [0,4,2,1,3]
    artist_data = artist_data = df[df.columns[artist_cols]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    Summary line
    ------------
    Process log files one by one and inserts the data to time, users, and songsplay table
    based on values from songs and artists table.
    
    Parameters
    ----------
    cur -- Database Cursor
    filepath -- JSON file path which has the user log data
    
    Returns
    -------
    None
    
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms') 
    
    # insert time data records
    time_data = [t, \
                 t.dt.hour, \
                 t.dt.day, \
                 t.dt.week, \
                 t.dt.month, \
                 t.dt.year, \
                 t.dt.dayofweek]
    column_labels = (['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday'])
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    cols = [17,2,5,3,7]
    user_df = df[df.columns[cols]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

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
        timestamp = pd.to_datetime(row.ts,unit='ms')
        songplay_data = (timestamp,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Summary line
    ------------
    Called by main function to get the filepath of the all song and log files and calls
    process_log_file and process_song_file to process and add rows to the songs, artits, users, 
    time, and songsplay tables
    
    Parameters
    ----------
    cur -- Database Cursor
    conn -- Database connection details
    filepath -- JSON file path which has the songs and log data
    func -- function name that will be called to process the file path
    
    Returns
    -------
    None
    
    """

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
    """
    Summary line
    ------------
    Main function which calls the process_data function to get the file path and call the other functions to process
    and insert data to the songs, artists, users, time, and songsplay tables
    
    Parameters
    ----------
    None
    
    Returns
    -------
    None

    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()