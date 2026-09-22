import unittest

from main import remember_publication_event


class PushEventPriorityTest(unittest.TestCase):
    def test_durable_push_row_survives_matching_synthetic_retry(self):
        events = {}
        key = ('abcdefghijk', 'SciTopus')
        source = {'row_index': 42, 'video_id': key[0]}
        retry = {'row_index': -1, 'video_id': key[0], 'retry_project': key[1]}

        remember_publication_event(events, key, source)
        remember_publication_event(events, key, retry)

        self.assertIs(events[key], source)

    def test_durable_push_row_replaces_earlier_synthetic_retry(self):
        events = {}
        key = ('abcdefghijk', 'SciTopus')
        retry = {'row_index': -1, 'video_id': key[0], 'retry_project': key[1]}
        source = {'row_index': 42, 'video_id': key[0]}

        remember_publication_event(events, key, retry)
        remember_publication_event(events, key, source)

        self.assertIs(events[key], source)
