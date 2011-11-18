"""
Classes responsible for retrieving raw data from the internet.
"""
from exceptions import NameError
import os
import urllib2
from hanalytics.fetchers.parlparse import log

from hanalytics.utils.worker import Worker


def fetch_url(url, error):
    try:
        handle = urllib2.urlopen(url)
        return handle.read()
    except urllib2.URLError:
        log.exception(error)
    finally:
        try:
            handle.close()
        except NameError:
            pass