import os
import unittest
import mock
from hanalytics.fetchers.parlparse import commons_speech_working_dir
from hanalytics.loaders.parlparse import commons_speech_feeder, commons_speech_saver
from tests import TestHelpMixin

class CommonsSpeechLoaderTest(unittest.TestCase, TestHelpMixin):
    def setUp(self):
        self.rootdir = "/tmp/hanalytics-test"
        self.writer = mock.Mock()
        self.working_dir = commons_speech_working_dir(self.rootdir)

    def tearDown(self):
        self.removedirs(self.rootdir)
        del self.rootdir, self.writer

    def test_feeder(self):
        open(os.path.join(self.working_dir, "debates2011-10-24b.xml"), "w+")
        open(os.path.join(self.working_dir, "debates2011-10-24c.xml"), "w+")
        tracker = os.path.join(self.working_dir, "tracker")
        with open(tracker, "w+") as f:
            f.writelines(["debates2011-10-24b.xml"])

        self.assertEqual(
            ['/tmp/hanalytics-test/parlparse/commons/debates2011-10-24c.xml'],
            list(commons_speech_feeder(self.working_dir, tracker))
        )

    def test_saver(self):
        path = self.fixture_path("hanalytics/loaders/debates2011-11-02a.xml")
        writer = mock.Mock()

        filename, count = commons_speech_saver(path, writer)
        self.assertEqual("debates2011-11-02a.xml", filename)
        self.assertEqual(519, count)
        self.assertEqual(519, writer.save.call_count)