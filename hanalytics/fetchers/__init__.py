"""
Classes responsible for retrieving raw data from the internet.
"""
import logging
from exceptions import NameError
import urllib2

log = logging.getLogger()


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