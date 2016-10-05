import scrapy
import csv
import sys
import sqlite3
from scrapy.crawler import CrawlerProcess

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
conn.close()

start = 7993
end = 8000


def get_errors(file):
    pass


def filter_errors(errors):
    pass


def write_count(num):
    count_file = open('./count.txt', 'w');
    count_file.write(str(num))
    count_file.close()


class ArtifactSpider(scrapy.Spider):
    name = 'chenyuluspider'
    start_urls = ['https://www.threadless.com/product/{}'.format(start)]
    handle_httpstatus_list = [
        400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417,
        500, 501, 502, 503, 504, 505
    ]

    def __init__(self):
        print "Initializing spider........."
        self.offset = start;
        self.current = self.offset
        print self.current
        self.limit = end
        self.base_url = 'https://www.threadless.com/product/'
        self.open();
        self.failed_queue = []
        self.interval = 10

    def insert_record(self):
        pass

    def open(self):
        # self.log = open('./log.txt', 'a')
        # self.csv_file = open('output.csv', 'a')
        # self.csv_writer = csv.writer(self.csv_file, delimiter=',',
        #                              quotechar='|', quoting=csv.QUOTE_MINIMAL)

        self.conn = sqlite3.connect('output.db')
        self.cursor = self.conn.cursor()

    def reopen(self):
        write_count(self.current)
        print "Closing file........."
        self.conn.commit()
        self.conn.close()
        # self.log.close()
        # self.csv_file.close()
        print "Reopening file.........."
        self.open()

    def parse(self, response):
        if response.status == 200:
            for author in response.css('.product_identity > .tip'):
                name = author.css('a ::text').extract_first()
                url = author.xpath('a/@href').extract_first()
                print self.current, name, url
                # self.csv_writer.writerow([self.current, name, url])
                self.cursor.execute('INSERT OR IGNORE  INTO artifacts(name, url, page_id) VALUES (?,?,?)', (
                    name, url, str(self.current)
                ))
        else:
            error_log = "Failed with ID {}, status code {}".format(self.current, response.status)
            print error_log
            self.cursor.execute('INSERT OR IGNORE  INTO errors(page_id, status_code) VALUES (?,?)', (
                str(self.current), response.status
            ))
            # self.log.write(error_log)
            # self.log.write('\n')
        if self.current < self.limit:
            self.current += 1
            if (self.current - self.offset) % self.interval == 0:
                self.reopen()
            yield scrapy.Request(response.urljoin(self.base_url + str(self.current)), self.parse)

    def closed(self, reason):
        self.reopen()


if __name__ == '__main__':
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    process.crawl(ArtifactSpider)
    process.start()  # the script will block here until the crawling is finished
