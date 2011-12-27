"""Fetching parlparse files"""
import logging
import os
import re
import urlparse
import multiprocessing as mp

# TODO: remove scrapy dependency, use lxml directly
from scrapy.selector.lxmlsel import HtmlXPathSelector

from hanalytics.fetchers import fetch_url, create_working_dir, commons_speech_saver

log = logging.getLogger()

def fetch_commons_speeches(root_dir, num_workers):
    """Fetch commons speeches from parlparse"""
    log.debug("starting fetcher")
    working_dir = commons_speech_working_dir(root_dir)
    pool = mp.Pool(num_workers, lambda *args: globals().update(dict(args)), {"_working_dir":working_dir}.items())
    counts = [0, 0]
    for result in pool.imap(commons_speech_saver, commons_speech_feeder(working_dir)):
        counts[0 if result else 1] += 1
        for i, count in enumerate(counts):
            if count and count % 1000 is 0:
                print("%s %s downloads" % (count, "bad" if i else "good"))

def commons_speech_working_dir(root_dir):
    """Create and return the working directory"""
    return create_working_dir(root_dir, "parlparse", "commons")

def commons_speech_feeder(working_dir, _fetch_url=None):
    """Return a generator that yields file urls"""
    # TODO: find a faster way of doing this
    if not _fetch_url:
        _fetch_url = fetch_url
    list_url = 'http://ukparse.kforge.net/parldata/scrapedxml/debates/'
    log.debug("Fetching index")
    data = _fetch_url(list_url, "Failed to fetch index.")
    if data:
        hxs = HtmlXPathSelector(text=unicode(data, errors="ignore"))
        selector = hxs.select(r'//table//td//a/@href')
        check_href = create_href_checker(re.compile(r'^debates\d{4}'), working_dir)
        urls = selector.extract()
        log.debug("Fetched %s urls from index" % len(urls))
        for href in urls:
            if check_href(href):
                yield urlparse.urljoin(list_url, href)

def create_href_checker(pattern, working_dir):
    """Return a function that can check if a url is valid"""
    file_list = os.listdir(working_dir)
    def check_href(href):
        """Return whether a url is vlaid or not"""
        if bool(pattern.match(href)):
            if os.path.basename(urlparse.urlparse(href).path) not in file_list:
                return True
        return False
    return check_href