import re
from datetime import datetime
import postges_helper as pg_helper
import difflib as dl
import schedule as schedule
import time

pg_helper.print_version()
pg_helper.create_missing_tables()


def word_frequency(string: str):
    string = string.split(" ")
    frequency = {}
    for s in string:
        if s in frequency:
            frequency[s] += 1
        else:
            frequency[s] = 1
    return frequency


def get_dif(s1: str, s2: str):
    """
    Removes matching parts of two strings from the first string. Creates lists of unmatched words.

    :param s1:
    :param s2:
    :return: [lowercase_words, uppercase_words] -> list[list[str]]
    """
    dif = dl.unified_diff(s1.split(" "), s2.split(" "))
    dif_str = " ".join(dif)
    dif_str = re.sub("@@ .*? @@", "", dif_str)
    dif_str = dif_str.replace("---", "").replace("+++", "")
    dif_str = dif_str.replace("\\", " ").replace("/", " ").replace("\"", " ")
    dif_list = dif_str.split(" ")

    clean_str = ""
    for d in dif_list:
        if d.startswith("-"):
            if clean_str == "":
                clean_str = d
            else:
                clean_str = clean_str + " " + d

    clean_str = clean_str.replace("+", " ").replace("-", " ")
    clean_str = clean_str.replace("\xa0", " ").replace("\n", " ").replace("\t", " ")
    clean_str = clean_str.replace("\u200b", " ")
    clean_str = clean_str.replace("!", " ").replace("?", " ").replace(". ", " ").replace(":", " ")

    word_list: list[str] = clean_str.split(" ")
    uppercase_words = []
    lowercase_words = []

    for w in word_list:
        try:
            if w[0].isupper():
                uppercase_words.append(w)
            elif w[0].islower():
                lowercase_words.append(w)
        except IndexError:
            pass

    return [lowercase_words, uppercase_words]


class Cleanup:
    def __init__(self, name):
        complete_news: tuple = pg_helper.get_tree_days_of_complete_news()
        news: dict[str, list[list]] = {}

        i = 0
        source_name = ""
        for row in complete_news:
            if row[2] != source_name:
                news[row[2]] = [row]
            else:
                old_list: list[list] = news.get(row[2])
                old_list.append(row)
                news[row[2]] = old_list
            source_name = row[2]

        for news_source in news:
            news_from_source: list[list[tuple]] = news[news_source]

            new_str_count = len(news_from_source)

            if new_str_count <= 1:
                print("Not enough data for cleanup for source: " + news_source)
                break
            else:
                median = new_str_count // 2
                compare_values: list[tuple[int, int]] = []

                i = 0
                while i <= median:
                    compare_values.append((i, new_str_count - 1))
                    i = i + 1
                while i < new_str_count:
                    compare_values.append((i, 0))
                    i = i + 1

                for v in compare_values:

                    str1 = str(news_from_source[v[0]][4])
                    str2 = str(news_from_source[v[1]][4])

                    wordlist = get_dif(str1, str2)

                    values = news_from_source[v[0]]

                    id_complete_news = int(values[0])
                    time_data = datetime.strptime(values[1], '%Y-%m-%d %H:%M:%S.%f')
                    source = str(values[2])
                    source_url = str(values[3])
                    content = " ".join(wordlist[1])

                    pg_helper.save_cleaned_news(id_complete_news, time_data, source, source_url, content)

            print("cleaned and saved data for " + news_source)


class Schedule:
    print("Initiating schedule at " + str(datetime.now()))
    schedule.every().day.at("04:05").do(Cleanup, 'Starting new run at 06:05')
    schedule.every().day.at("10:05").do(Cleanup, 'Starting new run at 12:05')
    schedule.every().day.at("16:05").do(Cleanup, 'Starting new run at 18:05')
    schedule.every().day.at("22:05").do(Cleanup, 'Starting new run at 00:05')

    while True:
        schedule.run_pending()
        time.sleep(60)
