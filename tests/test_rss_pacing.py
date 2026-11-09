import unittest
from concurrent.futures import ThreadPoolExecutor

from rss_discovery import RequestStartPacer


class RequestPacingTests(unittest.TestCase):
    def test_spacing_and_no_catchup_burst_after_a_pause(self):
        now = [10.0]
        delays = []

        def sleep(seconds):
            delays.append(seconds)
            now[0] += seconds

        pacer = RequestStartPacer(interval=.25, clock=lambda: now[0], sleep=sleep)
        pacer.wait()
        pacer.wait()
        pacer.wait()
        self.assertEqual(delays, [.25, .25])
        now[0] += 20
        pacer.wait()
        pacer.wait()
        self.assertEqual(delays, [.25, .25, .25])

    def test_parallel_starts_are_spaced(self):
        now = [10.0]
        delays = []

        def sleep(seconds):
            delays.append(seconds)
            now[0] += seconds

        pacer = RequestStartPacer(interval=.25, clock=lambda: now[0], sleep=sleep)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: pacer.wait(), range(8)))
        self.assertEqual(delays, [.25] * 7)
        self.assertEqual(now[0], 11.75)


if __name__ == '__main__':
    unittest.main()
