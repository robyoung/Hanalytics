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

def init_fetcher(*args):
    globals().update(dict(args))

def commons_speech_saver(url, _fetch_url=None, _working_dir=None):
    """Save a given url to a given location."""
    working_dir = _working_dir or globals()['_working_dir']
    data_checker = globals()['checker'] if "checker" in globals() else lambda data: True
    _fetch_url  = _fetch_url or fetch_url
    outpath = os.path.join(working_dir, os.path.basename(url))

    log.debug("Fetching %s" % url)
    data = _fetch_url(url, "Failed to download commons debate file.")
    if data:
        with open(outpath, "w+") as handle:
            if data_checker(data):
                handle.write(data)