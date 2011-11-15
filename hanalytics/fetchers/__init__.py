"""
Classes responsible for retrieving raw data from the internet.
"""
import os

from hanalytics.utils.worker import Worker

class Fetcher(Worker):
    def __init__(self, root_dir, subdir, num_workers):
        super(Fetcher, self).__init__(num_workers)
        self._root_dir = root_dir
        self._out_dir = os.path.join(root_dir, *subdir)
        if not os.path.exists(self._out_dir):
            os.makedirs(self._out_dir)