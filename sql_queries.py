create_table_songplays = "create table if not exists songplays(songplay_id text, start_time timestamp, user_id text, level text, song_id text, artist_id text, session_id int, location text, user_agent text)"
create_table_users = "create table if not exists users(user_id int, first_name text, last_name text, gender text, level text)"
create_table_songs = "create table if not exists songs(song_id text, title text, artist_id text, year int, duration float)"
create_table_artists = "create table if not exists artists(artist_id text, name text, location text, latitude text, longitude text)" 
create_table_time = "create table if not exists time(start_time timestamp, hour int, day int, week int, month int, year int, weekday text)"

drop_table_songplays = "drop table if exists songplays"
drop_table_users = "drop table if exists users"
drop_table_songs = "drop table if exists songs"
drop_table_artists = "drop table if exists artists"
drop_table_time = "drop table if exists time"

insert_table_songplays = "insert into songplays values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
insert_table_users = "insert into users values (%s,%s,%s,%s,%s)"
insert_table_songs = "insert into songs values (%s,%s,%s,%s,%s)"
insert_table_artists = "insert into artists values (%s,%s,%s,%s,%s)"
insert_table_time = "insert into time values (%s,%s,%s,%s,%s,%s,%s)"

drop_table_queries = [drop_table_songplays, drop_table_users, drop_table_songs, drop_table_artists, drop_table_time]
create_table_queries = [create_table_songplays, create_table_users, create_table_songs, create_table_artists, create_table_time]

