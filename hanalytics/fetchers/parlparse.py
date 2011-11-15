import logging
import os
import re
import urllib2
import urlparse

# TODO: remove scrapy dependency, use lxml directly
from scrapy.selector.lxmlsel import XmlXPathSelector

from hanalytics.fetchers import Fetcher

class CommonsSpeechFetcher(Fetcher):
    log = logging.getLogger()

    HREF_RE = re.compile(r'^debates\d{4}')

    def __init__(self, root_dir, num_workers):
        super(CommonsSpeechFetcher, self).__init__(root_dir, ["parlparse", "commons"], num_workers)

    def feeder(self):
        list_url = r'http://ukparse.kforge.net/parldata/scrapedxml/debates/'
        hxs = XmlXPathSelector(text=urllib2.urlopen(list_url).read())
        hrefs = hxs.select('//table//td//a/@href').extract()
        return (urlparse.urljoin(list_url,href) for href in hrefs if self.check_href(href))

    def check_href(self, href):
        return bool(self.HREF_RE.match(href))

    def do_work(self, url):
        self.log.debug("Fetching %s" % url)
        try:
            in_handle = urllib2.urlopen(url)
            with open(os.path.join(self._outdir, os.path.basename(url)), "w+") as out_handle:
                out_handle.write(in_handle.read())
        except urllib2.URLError as e:
            self.log.exception("Failed to download commons debate file.")
        finally:
            try:
                in_handle.close()
            except NameError:
                pass
        return True

    def read_results(self):
        counts = [0, 0]
        for result in self._outqueue:
            counts[0 if result else 1] += 1
            for i, count in enumerate(counts):
                if count and count % 1000 is 0:
                    self.log.info("%s %s downloads" % (count, "bad" if i else "good"))