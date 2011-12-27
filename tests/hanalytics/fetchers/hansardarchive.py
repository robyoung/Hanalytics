"""Unit tests for the hansard archive fetcher."""
import os
import unittest
import datetime
import mock
from tests import TestHelpMixin
from hanalytics.fetchers.hansardarchive import commons_speech_working_dir, commons_speech_feeder

class CommonsSpeechFetcherTest(unittest.TestCase, TestHelpMixin):
    """Unit test for fetching speeches"""
    def setUp(self):
        """Set root directory and mocks"""
        self.rootdir = "/tmp/hanalytics-test"
        self.writer = mock.Mock()

    def tearDown(self):
        """Delete created directories and any content"""
        self.removedirs(self.rootdir)
        del self.rootdir, self.writer

    def test_create_working_dir(self):
        """Test the working directory is correctly created"""
        working_dir = commons_speech_working_dir(self.rootdir)
        self.assertEqual(working_dir, "/tmp/hanalytics-test/hansardarchive")
        self.assertTrue(os.path.exists(working_dir))

    def test_feeder(self):
        """Test source urls are generated correctly and in quickly"""
        start = datetime.datetime.now()
        urls = [x for x in commons_speech_feeder(commons_speech_working_dir(self.rootdir))]
        self.assertEqual(len(urls), 995)
        self.assertLess(datetime.datetime.now() - start, datetime.timedelta(seconds=0.1))
        self.assertEqual(urls[0], "http://www.hansard-archive.parliament.uk/Parliamentary_Debates_1803_to_1820/S1V0001P0.zip")
        self.assertEqual(urls[-1], "http://www.hansard-archive.parliament.uk/Parliamentary_Debates_1909_to_1981/S5V0199P0.zip")
