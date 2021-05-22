import sqlite3
from sqlite3 import Connection, Error
from datetime import datetime


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Connected to Database")
    except Error as e:
        print(e)
    return conn


def create_missing_tables(conn: Connection):
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS news_complete("
              "id INTEGER PRIMARY KEY AUTOINCREMENT,"
              "time INTEGER,"
              "source STRING,"
              "source_url STRING,"
              "content STRING)")
    c.close()


def save_complete_news(conn: Connection, time: datetime, source: str, source_url: str, content: str):
    c = conn.cursor()
    data = (time, source, source_url, content)
    c.execute("INSERT INTO news_complete(time, source, source_url, content) VALUES (?,?,?,?)", data)
    conn.commit()
    c.close()
