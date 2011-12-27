"""Load hansard archive file into the database"""
import logging
import os
import multiprocessing as mp
import datetime
import zipfile

from scrapy.selector.lxmlsel import XmlXPathSelector

from hanalytics.fetchers.hansardarchive import commons_speech_working_dir
from hanalytics.loaders.parlparse import hxs_extract

log = logging.getLogger()

def load_commons_speeches(root_dir, writer, num_workers):
    log.debug("starting loader")
    working_dir = commons_speech_working_dir(root_dir)
    log.debug(working_dir)
    tracker = os.path.join(working_dir, "tracker")
    pool = mp.Pool(num_workers, lambda *args: globals().update(dict(args)), {"_writer":writer}.items())
    # TODO: move reporting out
    day_mod = 100
    day_count = 0
    day_time = datetime.datetime.now()
    speech_mod = 10000
    speech_count = 0
    speech_time = datetime.datetime.now()
    with open(tracker, "a+") as tracker_file:
        for filename, speeches in pool.imap(commons_speech_saver, commons_speech_feeder(working_dir, tracker)):
            day_count += 1
            if day_count % day_mod is 0:
                delta = datetime.datetime.now() - day_time
                log.info("%10s Days in %s (%s/s)" % (day_count, delta, day_count / delta.seconds))
                log.info("Latest file %s" % filename)
            for i in range((speech_count + speeches) / speech_mod - speech_count / speech_mod):
                sp_count = (speech_count / speech_mod + i + 1) * speech_mod
                delta = datetime.datetime.now() - speech_time
                log.info("%10s Speeches in %s (%s/s)" % (sp_count, delta, sp_count / delta.seconds))
                tracker_file.flush()
            speech_count += speeches
            tracker_file.write("%s\n" % filename)

def commons_speech_feeder(working_dir, tracker):
    # TODO: pull up function
    loaded = set(map(str.strip, open(tracker).readlines()) if os.path.exists(tracker) else [])
    loaded.add(os.path.basename(tracker))
    return (os.path.join(working_dir, filename) for filename in os.listdir(working_dir) if filename not in loaded)


def fix_bad_zipfile(path):
    # TODO: move out to utils module
    with open(path, "r+") as file:
        data = file.read()
        pos  = data.find('\x50\x4b\x05\x06')
        if pos > 0:
            file.seek(pos+22)
            file.truncate()
            file.close()
        else:
            raise zipfile.BadZipfile()

def commons_speech_saver(path, _writer=None):
    writer = _writer or globals()['_writer']

    try:
        count = 0
        try:
            fix_bad_zipfile(path)
            with zipfile.ZipFile(path) as zip_file:
                with zip_file.open(os.path.basename(path).replace(".zip", ".xml")) as inner_file:
                    text = unicode(inner_file.read(), errors="ignore")
                    hxs = XmlXPathSelector(text=text)
                    for housecommons in hxs.select(r'//housecommons'):
                        date_str = housecommons.select(r'.//date/@format').extract()[0]
                        speech_date = datetime.datetime.strptime(date_str.strip(), "%Y-%m-%d")

                        for speech in housecommons.select(r'.//p'):
                            writer.save({
                                "id": "hansardarchives/%s" % hxs_extract(speech, r'./@id'),
                                "house": "commons",
                                "source": "hansardarchives",
                                "speakerid": None,
                                "speakername": hxs_extract(speech, r'./member/text()'),
                                "column": None,
                                "date": speech_date,
                                "time": None,
                                "url": None,
                                "text": [hxs_extract(speech, r'./membercontribution/text()')]
                            })
                            count += 1
        except zipfile.BadZipfile:
            log.debug("Bad zip file %s" % os.path.basename(path))
        return os.path.basename(path), count
    except KeyboardInterrupt:
        log.warning("Caught exception in %s, sending stop" % os.getpid())
        raise StopIteration()
