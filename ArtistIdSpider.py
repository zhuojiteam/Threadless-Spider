import scrapy
import csv
# import beautifulsoup
import hmac
import hashlib
import base64
from scrapy.crawler import CrawlerProcess
import pandas
import sqlite3
import sys
from scrapy import Selector
import re

# sys.setdefaultencoding() does not exist, here!
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')

conn = sqlite3.connect('output.db')
cursor = conn.cursor()
cursor.execute(
    'CREATE TABLE IF NOT EXISTS artifacts (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'name TEXT, url TEXT, page_id TEXT, UNIQUE(page_id));')
cursor.execute(
    'CREATE TABLE IF NOT EXISTS errors (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'page_id TEXT, status_code INTEGER, UNIQUE(page_id));')
cursor.execute(
    'CREATE TABLE IF NOT EXISTS artists (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'artist_id TEXT, url INTEGER, name INTEGER,'
    'UNIQUE(artist_id), UNIQUE(url));')
cursor.execute(
    'CREATE TABLE IF NOT EXISTS artist_stats (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'artist_id TEXT, url INTEGER, name INTEGER,'
    'design_count INTEGER, design_scored INTEGER, avg_score DOUBLE, member_since INTEGER,'
    'UNIQUE(artist_id), UNIQUE(url));')
cursor.execute(
    'CREATE TABLE IF NOT EXISTS artist_errors (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'url TEXT, status_code INTEGER, error TEXT, UNIQUE(url));')
conn.close()

start = 0
end = 7898


def save(cursor):
    conn = cursor.connection
    conn.commit()
    conn.close()


def reget_cursor():
    conn = sqlite3.connect('output.db')
    return conn.cursor()


def build_artist_list():
    artist_list = []

    conn = sqlite3.connect('output.db')
    cursor = conn.cursor()
    artists = dict();
    cursor.execute('SELECT * FROM artifacts')
    result = cursor.fetchall()
    for row in result:
        artist = row[2]
        if artist not in artists:
            # if not artists.has_key(artist):
            artists[artist] = True
            artist_list.append(artist)
    print artist_list
    print artist_list.__len__()
    return artist_list[start:]


save_interval = 10


def build_url(artist):
    return 'https://www.threadless.com{}'.format(artist)


static_start_urls = map(build_url, build_artist_list())


class ArtistIdSpider(scrapy.Spider):
    name = 'chenyulu2spider'
    start_urls = static_start_urls
    handle_httpstatus_list = [
        400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417,
        500, 501, 502, 503, 504, 505
    ]

    def __init__(self):
        print static_start_urls
        self.cursor = reget_cursor()
        self.time_to_reopen = save_interval

    def parse(self, response):
        match = re.search(r"https://www.threadless.com(.*)", response.url)
        subpath = match.group(1)
        self.time_to_reopen -= 1
        if response.status == 200:
            selector = Selector(text=response.body)
            artist_id = selector.re('var profileId = (\d+);')[0]
            name = ''
            try:
                name = \
                    (response.css('.user-avatar > span.name ::text').extract_first()).rstrip('\n').strip()
            except Exception as err:
                self.cursor.execute('INSERT OR IGNORE INTO artist_errors(url,error) VALUES(?,?)', (
                    subpath, str(err)
                ))
            print artist_id, subpath, name
            self.cursor.execute('INSERT OR IGNORE INTO artists(artist_id, url, name) VALUES (?,?,?)', (
                artist_id, subpath, name
            ))
            design_count = '-1'
            design_score = '-1'
            avg_score = '-1.0'
            member_since = '-1'
            try:
                design_count = selector.re('([,\d]+) designs submitted')[0]
            except Exception:
                pass
            try:
                design_score = selector.re('([,\d]+) designs scored')[0]
            except Exception:
                pass
            try:
                avg_score = selector.re('Avg Score Given: ([.\d]+)')[0]
            except Exception:
                pass
            try:
                member_since = selector.re('Member since (\d+)')[0]
            except Exception:
                pass
            true_design_count = int(design_count.replace(',', ''))
            true_design_score = int(design_score.replace(',', ''))
            true_avg_score = float(avg_score)
            true_member_since = int(member_since)
            print true_design_count, true_design_score, true_avg_score, true_member_since
            self.cursor.execute(
                'INSERT OR IGNORE '
                'INTO artist_stats(artist_id, url, name, design_count, design_scored, avg_score, member_since)'
                'VALUES (?,?,?,?,?,?,?)',
                (
                    artist_id, subpath, name, true_design_count, true_design_count, true_avg_score, true_member_since
                ))
        else:
            print response
            self.cursor.execute('INSERT OR IGNORE INTO artist_errors(url,status_code) VALUES(?,?)', (
                subpath, response.status
            ))
            print "Failed with status" + str(response.status) + ", path/ID: " + subpath
        if self.time_to_reopen == 0:
            print "Saving!================================"
            save(self.cursor)
            self.time_to_reopen = save_interval
            self.cursor = reget_cursor()

        def closed(self, reason):
            save(self.cursor)


if __name__ == '__main__':
    build_artist_list()
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(ArtistIdSpider)
    process.start()  # the script will block here until the crawling is finished
