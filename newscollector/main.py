import asyncio
import re
import requests
from bs4 import BeautifulSoup
import postgres_helper
import time
import schedule as schedule
from datetime import datetime

print("Starting news collector")

urls: dict[str, str] = {
    "heute": "https://www.zdf.de/nachrichten/",
    "tagesschau": "https://www.tagesschau.de/",
    "sueddeutsche": "https://www.sueddeutsche.de/",
    "faz": "https://www.faz.net/aktuell/",
    "taz": "https://taz.de/",
    "zeit": "https://www.zeit.de/index",
    "welt": "https://www.welt.de/"
}


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
        print(text)
        postgres_helper.save_complete_news(timestamp, url, urls[url], text)


class Main:
    def __init__(self, name):
        self.name = name
        print(datetime.now())
        asyncio.run(main())


class Schedule:
    print("Initiating schedule at " + str(datetime.now()))
    schedule.every().day.at("04:00").do(Main, 'Starting new run at 06:00')
    schedule.every().day.at("10:00").do(Main, 'Starting new run at 12:00')
    schedule.every().day.at("16:00").do(Main, 'Starting new run at 18:00')
    schedule.every().day.at("22:00").do(Main, 'Starting new run at 00:00')

    while True:
        schedule.run_pending()
        time.sleep(60)
