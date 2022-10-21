import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    # open song file
    """ Providing a list of all song files in song_data that will be used to create dimensional song and artist tables
    """
    df = pd.read_json(filepath,lines=True)


    # insert song record
    """ Data from song_data dataset is extracted to insert into song record table 
"""
    song_data=list(df[['song_id','title','artist_id','year','duration']].values [0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    """ Data from song_data dataset is extracted to insert into artist table. The values are only being selected and converted from an array into a list """
    artist_data=list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    """ log file is read to extract data for time and user table  """
    df = pd.read_json(filepath,lines=True)
    df.head()

    # filter by NextSong action
    df = df = df[df['page']=='NextSong']
    df.head(1)

    # convert timestamp column to datetime
    """ timestamp is converted to correct value type to break down time stamp by hour, day, week, month, year and weekday"""
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    """Time data is instearted into time table"""
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    """ User table data is loaded from log_data dataset"""
    user_df = df[['userId', 'firstName','lastName','gender','level']]
    user_df
    # insert user records
    "Data is extracted and inserted to users table from sql_queries.py"
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    # insert songplay records
    """songplay records are extracted to create Fact table"""
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        """log_file doesn't specify ID for artist and song, both are queried from songs and artists tables"""
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        """Records are isnserted from sql_queries.py"""
        songplay_data =(pd.to_datetime(row.ts,unit='ms'),int(row.userId),row.level,
        songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


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
