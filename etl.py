import os 
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    
    df = pd.read_json(filepath, lines=True)

    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.values[0]
    cur.execute(insert_table_songs, song_data)

    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values[0]
    cur.execute(insert_table_artists, artist_data)

def process_log_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)

    df = df[df.page == 'NextSong']

    t = df.copy()

    t['ts'] = pd.to_datetime(t['ts'],unit='ms')

    time_data = (t.ts, t.ts.dt.hour, t.ts.dt.day, t.ts.dt.dayofweek, t.ts.dt.month, t.ts.dt.year, t.ts.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    time_dict = {"start_time" : t.ts, 
             "hour" : t.ts.dt.hour, 
             "day" : t.ts.dt.day, 
             "week" : t.ts.dt.dayofweek, 
             "month" : t.ts.dt.month, 
             "year" : t.ts.dt.year, 
             "weekday" : t.ts.dt.weekday}

    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(insert_table_time, list(row))

    user_data = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_data.iterrows():
        cur.execute(insert_table_users,list(row))

    for i, row in df.iterrows():
        result = cur.execute("select a.song_id, b.artist_id from songs a inner join artists b on a.artist_id = b.artist_id where a.title = %s and b.name = %s and a.duration = %s", (row.song, row.artist, row.length))
        result = cur.fetchone()

        if result:
            songid, artistid = result
        else:
            songid, artistid = None, None

        starttime = pd.to_datetime(row.ts,unit='ms')

        cur.execute(insert_table_songplays,(i, starttime, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent))


def process_data(cur, conn, filepath, func):
    filepath = "Data_Modeling_with_Postgres/"+filepath
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
    cur = conn.cursor()

    process_data(cur, conn, "data/song_data", process_song_file)
    process_data(cur, conn, "data/log_data", process_log_file)

    conn.close()

if __name__ == "__main__":
    main()
