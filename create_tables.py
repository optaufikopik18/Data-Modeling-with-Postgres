import psycopg2 
from sql_queries import *

def create_database():
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute("drop database if exists sparkifydb")
    cur.execute("create database sparkifydb with encoding 'utf8' template template0")

    cur.close()

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()