from datetime import datetime
import logging, os, re
import multiprocessing as mp

from scrapy.selector.lxmlsel import XmlXPathSelector

from hanalytics.fetchers.parlparse import commons_speech_working_dir

log = logging.getLogger()

def load_commons_speeches(root_dir, writer, num_workers):
    log.debug("starting loader")
    working_dir = commons_speech_working_dir(root_dir)
    log.debug(working_dir)
    tracker = os.path.join(working_dir, "tracker")
    pool = mp.Pool(num_workers, lambda *args: globals().update(dict(args)), {"_writer":writer}.items())
    day_mod = 100
    day_count = 0
    day_time = datetime.now()
    speech_mod = 10000
    speech_count = 0
    speech_time = datetime.now()
    with open(tracker, "a+") as tracker_file:
        for filename, speeches in pool.imap(commons_speech_saver, commons_speech_feeder(working_dir, tracker)):
            day_count += 1
            if day_count % day_mod is 0:
                delta = datetime.now() - day_time
                log.info("%10s Days in %s (%s/s)" % (day_count, delta, day_count / delta.seconds))
                log.info("Latest file %s" % filename)
            for i in range((speech_count + speeches) / speech_mod - speech_count / speech_mod):
                sp_count = (speech_count / speech_mod + i + 1) * speech_mod
                delta = datetime.now() - speech_time
                log.info("%10s Speeches in %s (%s/s)" % (sp_count, delta, sp_count / delta.seconds))
                tracker_file.flush()
            speech_count += speeches
            tracker_file.write("%s\n" % filename)

def commons_speech_feeder(working_dir, tracker):
    loaded = set(map(str.strip, open(tracker).readlines()) if os.path.exists(tracker) else [])
    loaded.add(os.path.basename(tracker))
    return (os.path.join(working_dir, filename) for filename in os.listdir(working_dir) if filename not in loaded)

RE_DATE = re.compile(r'\d{4}-\d{2}-\d{2}')
def commons_speech_saver(path, _writer=None):
    writer = _writer if _writer else globals()['_writer']

    try:
        with open(path) as file:
            # TODO: create my own lxml wrapper with convenience methods
            hxs = XmlXPathSelector(text=unicode(file.read(), errors="ignore"))
            count = 0
            for speech in hxs.select(r'//speech'):
                writer.save({
                    "id": hxs_extract(speech, r'./@id'),
                    "house": "commons",
                    "speakerid": hxs_extract(speech, "./@speakerid"),
                    "speakername": hxs_extract(speech, "./@speakername"),
                    "column": hxs_extract(speech, "./@column"),
                    "date": datetime.strptime(RE_DATE.search(path).group(0), "%Y-%m-%d"),
                    "time": hxs_extract(speech, "./@time"),
                    "url": hxs_extract(speech, "./@url"),
                    "text": speech.select("./*").extract()
                })
                count += 1
            return os.path.basename(path), count
    except KeyboardInterrupt:
        log.warning("Caught exception in %s, sending stop" % os.getpid())
        raise StopIteration()

def hxs_extract(selector, pattern):
    return "".join(selector.select(pattern).extract())