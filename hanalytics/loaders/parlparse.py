from datetime import datetime
import logging, os, re

from scrapy.selector.lxmlsel import XmlXPathSelector

from hanalytics.utils.worker import Worker

class CommonsSpeechLoader(Worker):
    log = logging.getLogger("hansard.loaders.parlparse")

    RE_DATE = re.compile("\d{4}-\d{2}-\d{2}")

    def __init__(self, root_dir, writer, num_workers=1):
        super(CommonsSpeechLoader, self).__init__(num_workers=num_workers)
        self._root_dir = root_dir
        self._writer = writer

    @property
    def tracker(self):
        return os.path.join(self._root_dir, "tracker")

    def feeder(self):
        loaded = set(map(str.strip, open(self.tracker).readlines()) if os.path.exists(self.tracker) else [])
        loaded.add("tracker")
        return (os.path.join(self._root_dir, filename) for filename in os.listdir(self._root_dir) if filename not in loaded)

    def do_work(self, path):
        with open(path, "r") as file:
            hxs = XmlXPathSelector(text=unicode(file.read(), errors="ignore"))
            count = 0
            for speech in hxs.select("//speech"):
                self._writer.save(self.get_speech(path, speech))
                count += 1
            return os.path.basename(path), count

    def get_speech(self, path, speech):
        return {
            "id": self.extract(speech, "./@id"),
            "house": "commons",
            "speakerid": self.extract(speech, "./@speakerid"),
            "speakername": self.extract(speech, "./@speakername"),
            "column": self.extract(speech, "./@column"),
            "date": datetime.strptime(self.RE_DATE.search(path).group(0), "%Y-%m-%d"),
            "time": self.extract(speech, "./@time"),
            "url": self.extract(speech, "./@url"),
            "text": speech.select("./*").extract()
        }

    def extract(self, selector, pattern):
        return "".join(selector.select(pattern).extract())

    def read_results(self):
        day_mod = 100
        speech_mod = 10000
        day_count = 0
        speech_count = 0
        with open(self.tracker, "a+") as tracker:
            for filename, speeches in self._outqueue:
                day_count += 1
                if day_count % day_mod is 0:
                    print "%10s Days" % day_count
                for i in range((speech_count + speeches) / speech_mod - speech_count / speech_mod):
                    print "%10s Speeches" % ((speech_count/speech_mod + i+1)*speech_mod,)
                    tracker.flush()
                speech_count += speeches
                tracker.write("%s\n" % filename)