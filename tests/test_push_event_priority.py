import unittest

from main import remember_publication_event, source_method_for_event


class PushEventPriorityTest(unittest.TestCase):
    def test_real_push_and_rss_keep_their_sources(self):
        self.assertEqual(source_method_for_event({'source': 'Push server'}, {}), 'Push')
        self.assertEqual(source_method_for_event({'source': 'RSS server'}, {}), 'RSS')

    def test_api_backup_is_not_reported_as_a_push(self):
        self.assertEqual(source_method_for_event({'source': 'YouTube API backup'}, {}),
                         'RSS [YouTube API backup]')
        self.assertEqual(source_method_for_event({'source': 'YouTube API backup'}, {'bot_only': True}),
                         'Bot: RSS [YouTube API backup]')

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
