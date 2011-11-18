from datetime import datetime, timedelta
import os
import unittest, mock
from hanalytics.fetchers.parlparse import commons_speech_working_dir, commons_speech_feeder, commons_speech_saver
from tests import TestHelpMixin

class CommonsSpeechFetcherTest(unittest.TestCase, TestHelpMixin):
    def setUp(self):
        self.rootdir = "/tmp/hanalytics-test"
        self.writer = mock.Mock()

    def tearDown(self):
        self.removedirs(self.rootdir)
        del self.rootdir, self.writer

    def test_create_working_dir(self):
        working_dir = commons_speech_working_dir(self.rootdir)
        self.assertEqual(working_dir, "/tmp/hanalytics-test/parlparse/commons")
        self.assertTrue(os.path.exists("/tmp/hanalytics-test/parlparse/commons"))

    def test_feeder(self):
        def fetch_index(url, _):
            return self.read_fixture("hanalytics/fetchers/parlparse-commons-index.html")

        start = datetime.now()
        urls = [x for x in commons_speech_feeder(commons_speech_working_dir(self.rootdir), fetch_index)]
        self.assertEqual(len(urls), 1000)
        self.assertLess(datetime.now() - start, timedelta(seconds=1))
        self.assertEqual(urls[0], u'http://ukparse.kforge.net/parldata/scrapedxml/debates/debates2008-06-30a.xml')
        self.assertEqual(urls[-1], u'http://ukparse.kforge.net/parldata/scrapedxml/debates/debates2011-11-14a.xml')

    def test_do_work(self):
        def fetch_file(url, _):
            return "fooo"
        commons_speech_saver("http://foo.com/foo/bar.html", fetch_file, commons_speech_working_dir(self.rootdir))
        path = os.path.join(commons_speech_working_dir(self.rootdir), "bar.html")
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            self.assertEqual(f.read(), "fooo")