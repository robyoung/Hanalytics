import logging
import os
import re
import pymongo

from scrapy.selector.lxmlsel import XmlXPathSelector
from hanalytics.loaders import Loader

class ParlparseCommonsDebatesLoader(Loader):
    log = logging.getLogger("hansard.loaders.parlparse")

    RE_DATE = re.compile("\d{4}-\d{2}-\d{2}")

    def __init__(self, root_dir, num_workers=1):
        super(ParlparseCommonsDebatesLoader, self).__init__(num_workers=num_workers)
        self._root_dir = root_dir

    @property
    def mongo(self):
        if not hasattr(self, "_mongo"):
            self.log.debug("No mongo")
            self._mongo = pymongo.Connection()['hanalytics']['debates']
        return self._mongo

    def feeder(self):
        return (os.path.join(self._root_dir, filename) for filename in os.listdir(self._root_dir))

    def do_load(self, path):
        with open(path, "r") as file:
            hxs = XmlXPathSelector(text=unicode(file.read(), errors="ignore"))
            speeches = [self.get_speech(path, speech) for speech in hxs.select("//speech")]
            if len(speeches):
                self.mongo.insert(speeches)
            return len(speeches)

    def get_speech(self, path, speech):
        return {
            "_id": self.extract(speech, "./@id"),
            "speakerid": self.extract(speech, "./@speakerid"),
            "speakername": self.extract(speech, "./@speakername"),
            "column": self.extract(speech, "./@column"),
            "date": self.RE_DATE.search(path).group(0),
            "time": self.extract(speech, "./@time"),
            "url": self.extract(speech, "./@url"),
            "paragraphs": speech.select("./*").extract()
        }

    def extract(self, selector, pattern):
        return "".join(selector.select(pattern).extract())

    def read_results(self):
        day_mod = 100
        speech_mod = 10000
        day_count = 0
        speech_count = 0
        for speeches in self._outqueue:
            day_count += 1
            if day_count % day_mod is 0:
                print "%10s Days" % day_count
            for i in range((speech_count + speeches) / speech_mod - speech_count / speech_mod):
                print "%10s Speeches" % ((speech_count/speech_mod + i+1)*speech_mod,)
            speech_count += speeches
