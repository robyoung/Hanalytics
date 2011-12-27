"""Fetching hansard archive files."""
import logging
import multiprocessing as mp
import os

from hanalytics.fetchers import  create_working_dir, commons_speech_saver

log = logging.getLogger()

def fetch_commons_speeches(root_dir, num_workers):
    """Fetch commons speeches from the hansard archives"""
    log.debug("starting fetcher")
    working_dir = commons_speech_working_dir(root_dir)
    pool = mp.Pool(num_workers, lambda *args: globals().update(dict(args)), {"_working_dir":working_dir}.items())
    counts = [0, 0]
    for result in pool.imap(commons_speech_saver, commons_speech_feeder(working_dir)):
        counts[0 if result else 1] += 1
        for i, count in enumerate(counts):
            if count and count % 1000 is 0:
                log.debug("%s %s downloads" % (count, "bad" if i else "good"))

def commons_speech_working_dir(root_dir):
    """Create and return the working directory"""
    return create_working_dir(root_dir, "hansardarchive")

def commons_speech_feeder(working_dir, _fetch_url=None):
    """Return a generator that yields file  urls"""
    all_series = [
        (1, 1803, 1820),
        (2, 1820, 1830),
        (3, 1830, 1891),
        (4, 1892, 1908),
        (5, 1909, 1981)
    ]
    tpl = "http://www.hansard-archive.parliament.uk/Parliamentary_Debates_%s_to_%s/S%sV%04iP0.zip"
    file_list = os.listdir(working_dir)

    for series, start_year, end_year in all_series:
        for volume in range(1, 200):
            url = tpl % (start_year, end_year, series, volume)
            if os.path.basename(os.path.basename(url)) not in file_list:
                yield url