import logging
import urllib2

from lxml import etree

from hanalytics.fetchers import Fetcher

class CommonsSpeechFetcher(Fetcher):
    log = logging.getLogger()

    def __init__(self, root_dir, num_workers):
        super(CommonsSpeechFetcher, self).__init__(root_dir, ["hansardarchive"], num_workers)

    def feeder(self):
        base_url = "http://www.hansard-archive.parliament.uk"
        path = "/Parliamentary_Debates_1803_to_1820/"
        urls = [
            "%s%sS1V00%02dP0.zip" % (base_url, path, i)
            for i in range(1, 9) + range(10, 16) + range(19, 23) + [24, 27, 30, 31, 35, 36, 37, 39, 40, 41]
        ]
        path = "/Parliamentary_Debates_1820_to_1830/"
        urls += [
            "%s%sS2V00%02dP0.zip" % (base_url, path, i)
            for i in range(1, 20) + range(22, 25)
        ]
        path = "/Parliamentary_Debates_1830_to_1891/"
        urls += [
            "%s%sS3V0%03dP0.zip" % (base_url, path, i)
            for i in range(1, 357)
        ]

        root_url = r'http://www.hansard-archive.parliament.uk/'
        tree = etree.parse(urllib2.urlopen(root_url), etree.HTMLParser())

        for session in tree.xpath(r'//a[@class="sessionlink"]/@href'):
            pass
        return []