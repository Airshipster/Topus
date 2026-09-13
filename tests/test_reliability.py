import hashlib
import hmac
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
import delivery_journal as journal
import push_store


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {'TOPUS_DELIVERY_DB': self.tmp.name + '/delivery.db',
                                          'TOPUS_PUSH_DB': self.tmp.name + '/push.db',
                                          'TOPUS_HUB_SECRET': 'test-secret-not-production',
                                          # This unit verifies the local receipt journal.  The
                                          # production coordinator is covered by its own tests.
                                          'TOPUS_CONTROL_URL': '',
                                          'TOPUS_CONTROL_REQUIRED': 'false',
                                          'TOPUS_PUBLISHER_OWNER': '',
                                          'TOPUS_PUBLISHER_LEASE': ''})
        self.env.start()
    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def test_claim_and_receipt_prevent_duplicate(self):
        self.assertEqual(journal.claim('video')['state'], 'claimed')
        self.assertEqual(journal.claim('video')['state'], 'sending')
        journal.finish('video', 'sent', 42)
        self.assertEqual(journal.claim('video')['message_id'], '42')

    def test_uncertain_is_not_retried(self):
        journal.claim('video')
        journal.finish('video', 'uncertain')
        self.assertEqual(journal.claim('video')['state'], 'uncertain')

    def test_rejected_send_can_retry_after_backoff(self):
        journal.claim('video')
        journal.finish('video', 'retry')
        self.assertEqual(journal.claim('video')['state'], 'retry')
        with journal.connection() as db:
            db.execute('UPDATE deliveries SET updated=0')
        self.assertEqual(journal.claim('video')['state'], 'claimed')

    def test_send_success_reuses_receipt_after_sheets_failure(self):
        response = Mock(ok=True)
        response.json.return_value = {'ok': True, 'result': {'message_id': 123}}
        with patch('requests.post', return_value=response) as post:
            self.assertEqual(journal.send_public('token', 'channel', 'text', 'project', 'video'), 123)
            self.assertEqual(journal.send_public('token', 'channel', 'text', 'project', 'video'), '123')
            self.assertEqual(post.call_count, 1)

    def test_verified_lease_not_just_accepted_request(self):
        channel = 'UC' + 'a' * 22
        with push_store.database() as db:
            db.execute('INSERT INTO leases(channel_id,requested) VALUES (?,?)', (channel, time.time()))
        self.assertFalse(push_store.confirm(channel, 'wrong', 3600))
        self.assertTrue(push_store.confirm(channel, push_store.verify_key(channel), 3600))
        self.assertEqual(push_store.health()['subscriptions']['verified_active'], 1)

    def test_signed_multi_entry_ingress_deduplicates(self):
        channel = 'UC' + 'a' * 22
        with push_store.database() as db:
            db.execute('INSERT INTO leases(channel_id) VALUES (?)', (channel,))
        xml = ('<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">'
               f'<entry><yt:videoId>abcdefghijk</yt:videoId><yt:channelId>{channel}</yt:channelId><updated>1</updated></entry>'
               f'<entry><yt:videoId>lmnopqrstuv</yt:videoId><yt:channelId>{channel}</yt:channelId><updated>1</updated></entry></feed>').encode()
        sig = 'sha1=' + hmac.new(b'test-secret-not-production', xml, hashlib.sha1).hexdigest()
        self.assertEqual(push_store.accept_xml(xml, sig), 2)
        self.assertEqual(push_store.accept_xml(xml, sig), 0)
        with self.assertRaises(PermissionError):
            push_store.accept_xml(xml, 'sha1=wrong')


if __name__ == '__main__':
    unittest.main()
