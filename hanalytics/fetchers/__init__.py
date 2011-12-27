"""
Classes responsible for retrieving raw data from the internet.
"""
import logging
from exceptions import NameError
import os
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


def create_working_dir(*parts):
    """Create and return the working directory"""
    working_dir = os.path.join(*parts)
    if not os.path.exists(working_dir):
        os.makedirs(working_dir)
    return working_dir