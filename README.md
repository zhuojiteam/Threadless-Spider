# Threadless Spider

A specific spider written for [threadless](https://www.threadless.com/)

Covering only some artists and some fields of the artists.

## Usage

It is compatible with Python2.

You may have to install `scrapy` first, e.g. via `pip install scrapy`. `sqlite3` is also necessary.

To run the spider, you can use `scrapy runspider <filename>` or simply `python <filename>`. Please run in the following order, since some spider depends on data crawled by other spiders. 

```shell
python ArtifactSpider.py
python ArtistIdSpider.py
python DetailSpider.py
```

`csv_to_sqlite.py` only serves to convert the legacy csv output format of the spider into the new sqlite tables, hence of no use now. 

## License

Licensed under GNU/GPLv3. Please use with care.