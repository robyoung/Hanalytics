"""Tests for loading hansard archive files"""
import os
import unittest
import mock
from tests import TestHelpMixin
from hanalytics.fetchers.hansardarchive import commons_speech_working_dir
from hanalytics.loaders.hansardarchive import commons_speech_feeder, commons_speech_saver

class CommonsSpeechLoaderTest(unittest.TestCase, TestHelpMixin):
    """Test loading commons speeches"""
    def setUp(self):
        """Create and set up working locations"""
        self.rootdir = "/tmp/hanalytics-test"
        self.writer  = mock.Mock()
        self.working_dir = commons_speech_working_dir(self.rootdir)

    def tearDown(self):
        """Remove working directory"""
        self.removedirs(self.rootdir)
        del self.writer

    def test_feeder(self):
        """Test the feeder correctly produces and filters files."""
        # TODO: merge with tests.hanalytics.loaders.parlparse
        open(os.path.join(self.working_dir, "S1V0001P0.zip"), "w+")
        open(os.path.join(self.working_dir, "S1V0002P0.zip"), "w+")
        tracker = os.path.join(self.working_dir, "tracker")
        with open(tracker, "w+") as f:
            f.writelines(["S1V0001P0.zip"])

        self.assertEqual(
            ['/tmp/hanalytics-test/hansardarchive/S1V0002P0.zip'],
            list(commons_speech_feeder(self.working_dir, tracker))
        )

    def test_saver(self):
        # TODO: merge with tests.hanalytics.loaders.parlparse
        path = self.fixture_path("hanalytics/loaders/S1V0001P0.zip")
        writer = mock.Mock()

        filename, count = commons_speech_saver(path, writer)
        self.assertEqual("S1V0001P0.zip", filename)
        self.assertEqual(1392, count)
        self.assertEqual(1392, writer.save.call_count)