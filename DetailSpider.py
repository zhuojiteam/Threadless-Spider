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
import re
import json

# sys.setdefaultencoding() does not exist, here!
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')

g_conn = sqlite3.connect('output.db')
g_cursor = g_conn.cursor()
g_cursor.execute(
    'CREATE TABLE IF NOT EXISTS product_count (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'artist_id INTEGER, product_count INTEGER, UNIQUE(artist_id));')
g_cursor.execute(
    'CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'artist_id INTEGER, json TEXT, page_number INTEGER, page_size INTEGER, UNIQUE(artist_id, page_number, page_size));')
g_cursor.execute(
    'CREATE TABLE IF NOT EXISTS product_errors (id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'artist_id INTEGER, status_code INTEGER, error TEXT, request TEXT, UNIQUE(request));')
g_conn.close()

start = 0
end = 7898


def build_artist_id_list():
    artist_id_list = []
    conn = sqlite3.connect('output.db')
    cursor = conn.cursor()
    artist_id_dict = dict();
    cursor.execute('SELECT * FROM artists')
    result = cursor.fetchall()
    for row in result:
        artist_id = row[1]
        if artist_id not in artist_id_dict:
            # if not artists.has_key(artist):
            artist_id_dict[artist_id] = True
            artist_id_list.append(artist_id)
    print artist_id_list
    print artist_id_list.__len__()
    return artist_id_list[start:]


save_interval = 10
g_page_size = 36
product_count = dict()


def build_url(artist_id, page_size, page_number):
    return 'https://loom.threadless.com/users/{}/products?per_page={}&page={}'.format(artist_id, page_size, page_number)


def build_start_urls(artist_id):
    product_count[int(artist_id)] = 0
    return build_url(artist_id, g_page_size, 0)


static_start_urls = map(build_start_urls, build_artist_id_list())

print static_start_urls


def save(cursor):
    conn = cursor.connection
    conn.commit()
    conn.close()


def reget_cursor():
    conn = sqlite3.connect('output.db')
    return conn.cursor()


def parse_url(u):
    match = re.search(
        r'https://loom\.threadless\.com/users/(\d+)/products\?per_page=(\d+)&page=(\d+)', u)
    artist_id_str = match.group(1)
    page_size_str = match.group(2)
    page_number_str = match.group(3)

    path = '/users/{}/products?per_page={}&page={}'.format(artist_id_str, page_size_str, page_number_str)

    artist_id = int(artist_id_str)
    page_size = int(page_size_str)
    page_number = int(page_number_str)

    return path, artist_id, page_size, page_number


secret = 'da0f8b91dfe8f587af014a6d52bc0dece93dfe07'
token = 'threadless'
date = 'Thu, 29 Sep 2016 18:24:58 GMT'
contentMd5 = '1B2M2Y8AsgTpgAmY7PhCfg=='


def build_header(u):
    path, artist_id, page_size, page_number = parse_url(u)

    tosign = 'GET' + '\n' + contentMd5 + '\n' + date + '\n' + path;
    digest = hmac.new(secret, msg=tosign, digestmod=hashlib.sha256).digest()

    signature = base64.b64encode(digest).decode()

    header = {
        'Content-MD5': contentMd5,
        'X-Authorization':
            'Threadless {{\"token\":\"threadless\",\"signature\":\"{}\",\"username\":null,\"user_token\":null}}'.format(
                signature), 'X-Date': date
    }
    return header


class DetailSpider(scrapy.Spider):
    name = 'chenyulu2spider'
    start_urls = static_start_urls
    handle_httpstatus_list = [
        400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417,
        500, 501, 502, 503, 504, 505
    ]

    def __init__(self):

        self.cursor = reget_cursor()
        self.time_to_reopen = save_interval
        pass

    def start_requests(self):
        for u in self.start_urls:
            print u
            header = build_header(u)
            print "header is ", header
            yield scrapy.Request(u, headers=header)

    def parse(self, response):
        path, artist_id, page_size, page_number = parse_url(response.url)
        self.time_to_reopen -= 1
        if response.status == 200:
            print response.body
            products = json.loads(response.body)
            l = products.__len__()
            print "Length of products is ", l
            self.cursor.execute(
                'INSERT OR IGNORE INTO products (artist_id, json, page_number, page_size) VALUES (?,?,?,?)', (
                    artist_id, response.body, page_number, page_size
                ))
            product_count[artist_id] += l
            if l == g_page_size:
                next_url = build_url(artist_id, page_size, page_number + 1)
                headers = build_header(next_url)
                yield scrapy.Request(next_url, headers=headers)
            else:
                self.cursor.execute('INSERT OR IGNORE INTO product_count (artist_id, product_count) VALUES(?,?)', (
                    artist_id, product_count[artist_id]
                ))
        else:
            print "Failed with request: " + response.url
            self.cursor.execute(
                'INSERT OR IGNORE INTO product_errors (artist_id, status_code, request) VALUES (?,?,?)', (
                    artist_id, response.status, response.url
                ))
        if self.time_to_reopen == 0:
            print "Saving!================================"
            save(self.cursor)
            self.time_to_reopen = save_interval
            self.cursor = reget_cursor()

    def closed(self, reason):
        save(self.cursor)


if __name__ == '__main__':
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(DetailSpider)
    process.start()  # the script will block here until the crawling is finished


# What do I need/want to do now?
def patch():
    pass
