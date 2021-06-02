from datetime import datetime
import pg8000.dbapi
import pg8000.native
from pg8000 import Connection, DatabaseError
from config import config


def print_version():
    params = config()
    print("Connected to database")
    conn = pg8000.native.Connection(**params)
    for row in conn.run('SELECT version()'):
        print(row)
    conn.close()


def create_missing_tables():
    params = config()
    print("Connected to database")
    conn = pg8000.native.Connection(**params)
    conn.run("CREATE TABLE IF NOT EXISTS news_complete("
             "id SERIAL PRIMARY KEY,"
             "time TEXT,"
             "source TEXT,"
             "source_url TEXT,"
             "content TEXT)")
    conn.run("CREATE TABLE IF NOT EXISTS news_sources("
             "id SERIAL PRIMARY KEY,"
             "source TEXT,"
             "source_url TEXT)")
    conn.close()
    print("Created missing database tables")


def get_news_sources():
    try:
        params = config()
        conn: Connection = pg8000.dbapi.Connection(**params)
        cur = conn.cursor()
        cur.execute("SELECT source, source_url FROM news_sources")
        r: tuple = cur.fetchall()
        cur.close()
        conn.close()
        return r
    except DatabaseError:
        print("Database Error // news_sources table could be missing in db // terminating")
        quit()


def save_complete_news(time: datetime, source: str, source_url: str, content: str):
    data = (time, source, source_url, content)
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("INSERT INTO news_complete(time, source, source_url, content) VALUES (%s, %s, %s, %s)", data)
    conn.commit()
    cur.close()
    conn.close()
