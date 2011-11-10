import multiprocessing as mp
from Queue import Empty
import unittest

class SentinelQueue(object):
    def __init__(self, maxsize=0, processes=None):
        self._queue = mp.Queue(maxsize)
        self._num_processes = mp.cpu_count() if processes is None else processes

    def __getattr__(self, item):
        return getattr(self._queue, item)

    def send_sentinel(self):
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()

class SentinelInQueue(SentinelQueue):
    def send_sentinel(self):
        map(self._queue.put, [StopIteration() for _ in range(self._num_processes)])

    def __iter__(self):
        get = self._queue.get
        while True:
            try:
                item = get()
                if isinstance(item, StopIteration):
                    break
                yield item
            except Empty:
                pass

class SentinelOutQueue(SentinelQueue):
    def send_sentinel(self):
        self._queue.put(StopIteration())

    def __iter__(self):
        get = self._queue.get
        stops = 0
        while stops < self._num_processes:
            try:
                item = get()
                if isinstance(item, StopIteration):
                    stops += 1
                else:
                    yield item
            except Empty:
                pass

class SentinelInQueueTest(unittest.TestCase):
    def setUp(self):
        self._queue = SentinelInQueue(15, 5)

    def tearDown(self):
        del self._queue

    def test_basic_queue(self):
        map(self._queue.put, ['a', 'b', 'c'])

        self.assertEqual(self._queue.get(), 'a')
        self.assertEqual(self._queue.get(), 'b')
        self.assertEqual(self._queue.get(), 'c')
        self.assertRaises(Empty, self._queue.get_nowait)

    def test_send_sentinel(self):
        self._queue.put('a')
        self._queue.send_sentinel()
        self.assertEqual(self._queue.get(), 'a')
        self.assertIsInstance(self._queue.get(), StopIteration)
        self.assertIsInstance(self._queue.get(), StopIteration)
        self.assertIsInstance(self._queue.get(), StopIteration)
        self.assertIsInstance(self._queue.get(), StopIteration)
        self.assertIsInstance(self._queue.get(), StopIteration)
        self.assertRaises(Empty, self._queue.get_nowait)

    def test_iter(self):
        self._queue.put('a')
        self._queue.send_sentinel()

        iter = self._queue.__iter__()
        self.assertEqual(iter.next(), 'a')
        self.assertRaises(StopIteration, iter.next)
