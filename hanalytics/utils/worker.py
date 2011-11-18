"""
A simple framework for parallelising workloads.
"""
import logging
import itertools
import multiprocessing as mp


from hanalytics.utils.queue import SentinelInQueue, SentinelOutQueue

# TODO: refactor this to make workers more easily testable
class Worker(object):
    log = logging.getLogger()

    def __init__(self, num_workers):
        self._num_workers = num_workers

    def start(self):
        self._start_queues()
        self._start_workers()
        self._start_feeder()
        self.read_results()

    def _start_queues(self):
        self._inqueue = SentinelInQueue(100, self._num_workers)
        self._outqueue = SentinelOutQueue(100, self._num_workers)

    def _start_workers(self):
        self._workers = [mp.Process(target=self.run, name="Loader %s" % (i+1,)) for i in range(self._num_workers)]
        [worker.start() for worker in self._workers]

    def _start_feeder(self):
        def feeder():
            any(itertools.imap(self._inqueue.put, self.feeder()))
            self._inqueue.send_sentinel()
        self._feeder = mp.Process(target=feeder, name="Feeder")
        self._feeder.start()

    def read_results(self):
        count = 0
        for _ in self._outqueue:
            count += 1
            if count % 1000 is 0:
                self.log.debug("%s" % count)

    def run(self):
        for message in self._inqueue:
            self._outqueue.put(self.do_work(message))
        self._outqueue.send_sentinel()

    def feeder(self):
        raise NotImplementedError()

    def do_work(self, message):
        raise NotImplementedError()
