import os
import tempfile
import unittest
from unittest.mock import patch
from channel_availability import record, unavailable, check


class AvailabilityTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {'TOPUS_PUSH_DB': self.directory.name + '/push.db'})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.directory.cleanup()

    def test_two_separated_confirmations_and_recovery(self):
        record('missing', False, 100000)
        record('missing', False, 100001)
        self.assertEqual(unavailable(100001), set())
        record('missing', False, 186400)
        self.assertEqual(unavailable(186400), {'missing'})
        record('missing', True, 186401)
        self.assertEqual(unavailable(186401), set())

    def test_stale_evidence_is_not_a_ban(self):
        record('missing', False, 100000)
        record('missing', False, 186400)
        self.assertEqual(unavailable(400000), set())

    def test_api_failure_does_not_mark_missing(self):
        with patch('config.YOUTUBE_API_KEYS', ['test']), patch('api_rescue.request', side_effect=RuntimeError('503')):
            check({'alive'})
        self.assertEqual(unavailable(), set())
