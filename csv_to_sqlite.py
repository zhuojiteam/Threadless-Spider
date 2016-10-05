#!/usr/bin/python

import csv
import sys
import sqlite3
import csv, codecs, cStringIO
from scrapy.crawler import CrawlerProcess
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
conn.close()

save_interval = 10


def save(cursor):
    conn = cursor.connection
    conn.commit()
    conn.close()


def reget_cursor():
    conn = sqlite3.connect('output.db')
    return conn.cursor()


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """

    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


def import_records():
    time_to_reopen = save_interval
    cursor = reget_cursor()
    output_file = open('./output.csv')
    output_csv = UnicodeReader(output_file, encoding='latin-1')
    for row in output_csv:
        time_to_reopen -= 1
        # page_id, name, url = *row
        page_id = row[0]
        name = row[1]
        url = row[2]
        print name, url, page_id
        cursor.execute('INSERT OR IGNORE INTO artifacts (name, url, page_id) VALUES (?,?,?)', (name, url, page_id))
        if time_to_reopen == 0:
            print "Saving!================================"
            save(cursor)
            time_to_reopen = save_interval
            cursor = reget_cursor()


def import_errrors():
    time_to_reopen = save_interval
    cursor = reget_cursor()
    output_file = open('./log.txt')
    for line in output_file:
        time_to_reopen -= 1
        print line
        match = re.search(r"Failed with ID (\d+), status code (\d+)", line)
        if (match):
            print match.group(0, 1, 2)
            cursor.execute('INSERT OR IGNORE INTO errors (page_id, status_code) VALUES (?,?)', (
                match.group(1), match.group(2)
            ))
        if time_to_reopen == 0:
            print "Saving!================================"
            save(cursor)
            time_to_reopen = save_interval
            cursor = reget_cursor()


if __name__ == '__main__':
    # import_records()
    import_errrors()