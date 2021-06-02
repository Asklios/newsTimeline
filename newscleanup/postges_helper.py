import re
from datetime import datetime
import pg8000.dbapi
from pg8000 import Connection

from config import config


def print_version():
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute('SELECT version()')
    print("Connected to database")
    print(cur.fetchone())
    conn.close()


def get_complete_news():
    """
    Selects all data from news_complete
    [id, time, source, source_url, content]

    :return: result tuple, can be empty if no stored news are available
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id, time, source, source_url, content FROM news_complete ORDER BY id")
    result: tuple = cur.fetchall()
    cur.close()
    return result


def get_all_sources():
    """
    Distinct sources from complete_news
    [source]

    :return: tuple of ordered sources
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT(source) FROM news_complete ORDER BY source")
    result = cur.fetchall()
    cur.close()
    return result


def get_tree_days_of_complete_news():
    """
    Latest 12 entries in complete_news for each source, ordered by source, time
    [id, time, source, source_url, content]

    :return: tuple of up to 12 rows for each source ordered by source/time, can be empty if there is no data
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("With LastRows AS ( " +
                "SELECT id, time, source, source_url, content, ROW_NUMBER() " +
                "OVER ( " +
                "PARTITION BY source " +
                "ORDER BY time DESC " +
                ") AS RowNumber " +
                "FROM news_complete) " +

                "SELECT * FROM LastRows " +
                "WHERE LastRows.RowNumber <= 12 " +
                "ORDER BY source ASC, time;")
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_complete_news_by_source(source: str):
    """
    Source complete_news ordered by time
    [id, time, source, source_url, content]

    :param source: Name of source
    :return: tuple of rows from source ordered by time, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id, time, source, source_url, content FROM news_complete WHERE source=%s "
                "ORDER BY time ASC", (source,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_latest_complete_news_by_source(source: str):
    """
    Latest complete_news from source
    [id, time, source, source_url, content]

    :param source: Name of source
    :return: tuple of row from source, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id, time, source, source_url, content FROM news_complete WHERE source=%s "
                "ORDER BY time ASC LIMIT 1", (source,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_one_day_of_complete_news_by_source(source: str):
    """
    Latest 4 entries in complete_news from source
    [id, time, source, source_url, content]

    :param source: Name of source
    :return: tuple of 4 rows from source ordered by time, can contain less than 4 rows, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id, time, source, source_url, content FROM news_complete WHERE source=%s "
                "ORDER BY time ASC LIMIT 4", (source,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_tree_days_of_complete_news_by_source(source: str):
    """
    Latest 12 entries in complete_news from source
    [id, time, source, source_url, content]

    :param source: Name of source
    :return: tuple of 12 rows from source ordered by time, can contain less than 12 rows, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id, time, source, source_url, content FROM news_complete WHERE source=%s "
                "ORDER BY time ASC LIMIT 12", (source,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def get_one_week_of_complete_news_by_source(source: str):
    """
    Latest 28 entries in complete_news from source
    [id, time, source, source_url, content]

    :param source: Name of source
    :return: tuple of 28 rows from source ordered by time, can contain less than 28 rows, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id, time, source, source_url, content FROM news_complete WHERE source=%s "
                "ORDER BY time ASC LIMIT 28", (source,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def __is_date(date: str):
    """
    Checks if String could represent a date, checks string pattern only
    [id, time, source, source_url, content]

    :param date: yyyy-MM-dd e.g. 2021-01-20
    :return: true if pattern is valid, otherwise false
    """
    pattern = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    if pattern.match(date):
        return True
    else:
        return False


def get_complete_news_by_date(date: str):
    """
    All complete_news entries of the specified date
    [id, time, source, source_url, content]

    :param date: yyyy-MM-dd e.g. 2021-01-20
    :return: Tuple of rows on the given date, ordered by time, can be empty if there are no entries, is None if the date pattern is false
    """
    if not __is_date(date):
        return None
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id, time, source, source_url, content FROM news_complete WHERE time ~ %s "
                "ORDER BY time", (date,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def create_missing_tables():
    """
    Adds possibly missing tables to the database. [news_clean_uppercase]
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS news_clean_uppercase("
                "id SERIAL PRIMARY KEY,"
                "id_complete_news BIGINT,"
                "time TEXT,"
                "source TEXT,"
                "source_url TEXT,"
                "content TEXT)")
    conn.commit()
    cur.close()
    conn.close()
    print("Created missing database tables")


def save_cleaned_news(id_complete_news, time: datetime, source: str, source_url: str, content: str):
    """
    Inserts one row into the news_clean_uppercase table. Ignores input if the id_complete_news exists.

    :param id_complete_news: row id from news_complete
    :param time: datetime from news_complete
    :param source: source from news_complete
    :param source_url: source_url from news_complete
    :param content: cleaned uppercase content
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT id_complete_news FROM news_clean_uppercase WHERE id_complete_news=%s", (id_complete_news,))
    r = cur.fetchone()

    if r is None:
        cur.close()

        cur = conn.cursor()
        data = (id_complete_news, time, source, source_url, content)
        cur.execute("INSERT INTO news_clean_uppercase(id_complete_news, time, source, source_url, content) "
                    "VALUES (%s, %s, %s, %s, %s)", data)
        conn.commit()
        cur.close()
        conn.close()
