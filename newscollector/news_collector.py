import asyncio
import re
import requests
from bs4 import BeautifulSoup
import time
import schedule as schedule
from datetime import datetime

import postgres_helper
import news_cleanup

print("Starting news collector")

urls: dict[str, str] = {}

postgres_helper.create_missing_tables()
sources = postgres_helper.get_news_sources()
for s in sources:
    urls[s[0]] = s[1]

print("looking for " + str(len(urls)) + " sources")


async def get_words_from_url(url):
    req = requests.get(url).content
    req.decode('ISO-8859-1')
    parsed_html = BeautifulSoup(req, features="html.parser")
    text = parsed_html.text
    text = text.replace("\n", " ")
    text = re.sub(' +', ' ', text)
    return text


async def main():
    postgres_helper.print_version()
    postgres_helper.create_missing_tables()
    timestamp: datetime = datetime.now()
    for url in urls:
        print("reading news from \"" + url + "\".")
        text = await get_words_from_url(urls[url])
        postgres_helper.save_complete_news(timestamp, url, urls[url], text)


class Main:
    def __init__(self, name):
        self.name = name
        print("---Start new run---")
        print(datetime.now())
        asyncio.run(main())
        print("starting cleanup")
        news_cleanup.cleanup()
        print("---Run finished---")


class Schedule:
    print("Initiating schedule at " + str(datetime.now()))
    schedule.every().day.at("04:00").do(Main, 'Starting new run at 06:00')
    schedule.every().day.at("10:00").do(Main, 'Starting new run at 12:00')
    schedule.every().day.at("16:00").do(Main, 'Starting new run at 18:00')
    schedule.every().day.at("22:00").do(Main, 'Starting new run at 00:00')

    while True:
        schedule.run_pending()
        time.sleep(60)
