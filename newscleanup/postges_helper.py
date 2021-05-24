from typing import Pattern

import pg8000.dbapi
from pg8000 import Connection
from config import config
import re


def get_complete_news():
    """
    Selects all data from news_complete

    :return: result tuple, can be empty if no stored news are available
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT * FROM news_complete ORDER BY id")
    result: tuple = cur.fetchall()
    cur.close()
    return result


def get_all_sources():
    """
    Distinct sources from complete_news

    :return: tuple of ordered sources
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT(source) FROM news_complete ORDER BY source")
    result = cur.fetchall()
    cur.close()
    return result


def get_complete_news_by_source(source: str):
    """
    Source complete_news ordered by time

    :param source: Name of source
    :return: tuple of rows from source ordered by time, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT * FROM news_complete WHERE source=%s ORDER BY time ASC", (source,))
    result = cur.fetchall()
    cur.close()
    return result


def get_latest_complete_news_by_source(source: str):
    """
    Latest complete_news from source

    :param source: Name of source
    :return: tuple of row from source, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT * FROM news_complete WHERE source=%s ORDER BY time ASC LIMIT 1", (source,))
    result = cur.fetchall()
    cur.close()
    return result


def get_one_day_of_complete_news_by_source(source: str):
    """
    Latest 4 entries in complete_news from source

    :param source: Name of source
    :return: tuple of 4 rows from source ordered by time, can contain less than 4 rows, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT * FROM news_complete WHERE source=%s ORDER BY time ASC LIMIT 4", (source,))
    result = cur.fetchall()
    cur.close()
    return result


def get_one_week_of_complete_news_by_source(source: str):
    """
    Latest 28 entries in complete_news from source

    :param source: Name of source
    :return: tuple of 28 rows from source ordered by time, can contain less than 28 rows, can be empty if source does not exist
    """
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT * FROM news_complete WHERE source=%s ORDER BY time ASC LIMIT 28", (source,))
    result = cur.fetchall()
    cur.close()
    return result


def __is_date(date: str):
    """
    Checks if String could represent a date, checks string pattern only

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

    :param date: yyyy-MM-dd e.g. 2021-01-20
    :return: Tuple of rows on the given date, ordered by time, can be empty if there are no entries, is None if the date pattern is false
    """
    if not __is_date(date):
        return None
    params = config()
    conn: Connection = pg8000.dbapi.Connection(**params)
    cur = conn.cursor()
    cur.execute("SELECT * FROM news_complete WHERE time ~ %s ORDER BY time", (date,))
    result = cur.fetchall()
    cur.close()
    return result
