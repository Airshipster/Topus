import os
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlencode

import controller
from push_store import accept_xml, database, health, record_callback
from rss_discovery import push_gap_health, save_result
from renew_direct import require_verified_coverage


class CoverageTests(unittest.TestCase):
    def test_callback_health_tracks_delivery_without_payload_data(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'TOPUS_PUSH_DB': directory + '/push.db'}):
            record_callback('accepted', counts={'entries': 3, 'new_events': 2,
                                               'duplicate_events': 1})
            record_callback('rejected', rejection_code='bad_signature')
            callbacks = health()['callbacks']
        self.assertEqual(callbacks['requests'], 2)
        self.assertEqual(callbacks['accepted'], 1)
        self.assertEqual(callbacks['rejected'], 1)
        self.assertEqual(callbacks['entries'], 3)
        self.assertEqual(callbacks['new_events'], 2)
        self.assertEqual(callbacks['duplicate_events'], 1)
        self.assertEqual(callbacks['last_rejection_code'], 'bad_signature')

    def test_acceptance_does_not_replace_verification(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'TOPUS_PUSH_DB': directory + '/push.db'}):
            with database() as db:
                db.execute("INSERT INTO leases(channel_id,requested) VALUES ('pending',?)", (time.time(),))
                db.execute("INSERT INTO leases(channel_id,enabled) VALUES ('disabled',0)")
            with self.assertRaisesRegex(RuntimeError, 'WEBSUB_UNVERIFIED_1'):
                require_verified_coverage()
            with database() as db:
                db.execute('UPDATE leases SET expires=? WHERE channel_id=?', (time.time()+3600, 'pending'))
            require_verified_coverage()

    def test_canonical_and_legacy_topics_keep_signature_check(self):
        for path in ('/feeds/videos.xml', '/xml/feeds/videos.xml'):
            query = urlencode({'hub.topic': 'https://www.youtube.com'+path+'?channel_id=UCfixture',
                               'hub.mode': 'subscribe', 'hub.lease_seconds': '3600',
                               'hub.challenge': 'challenge', 'verify': 'signed-token'})
            request = Mock(path='/websub?'+query)
            with patch.object(controller, 'confirm', return_value=True) as confirm:
                controller.Handler.do_GET(request)
                confirm.assert_called_once_with('UCfixture', 'signed-token', 3600)
                request.reply.assert_called_once_with(200, 'challenge', text=True)
            request.reset_mock()
            with patch.object(controller, 'confirm', return_value=False):
                controller.Handler.do_GET(request)
                request.reply.assert_called_once_with(403, 'rejected', text=True)

    def test_foreign_topic_never_confirms(self):
        request = Mock(path='/websub?'+urlencode({'hub.topic':'https://example.com/feeds/videos.xml',
                       'hub.mode':'subscribe','hub.lease_seconds':'3600'}))
        with patch.object(controller, 'confirm') as confirm:
            controller.Handler.do_GET(request)
            confirm.assert_not_called()
            request.reply.assert_called_once_with(403, 'rejected', text=True)

    def test_late_push_resolves_rss_observation(self):
        xml = (b'<feed xmlns="http://www.w3.org/2005/Atom" '
               b'xmlns:yt="http://www.youtube.com/xml/schemas/2015"><entry>'
               b'<yt:videoId>abcdefghijk</yt:videoId>'
               b'<yt:channelId>UCabcdefghijklmnopqrstuv</yt:channelId>'
               b'<updated>2026-09-25T00:00:00Z</updated></entry></feed>')
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                'TOPUS_PUSH_DB': directory + '/push.db', 'TOPUS_HUB_SECRET': 'fixture'}):
            with database() as db:
                db.execute("INSERT INTO leases(channel_id,enabled) VALUES (?,1)",
                           ('UCabcdefghijklmnopqrstuv',))
            save_result('UCabcdefghijklmnopqrstuv', [], '', now=100)
            save_result('UCabcdefghijklmnopqrstuv', [{'video_id':'abcdefghijk'}], '',
                        now=200, track_push_gap=True)
            import hashlib, hmac
            signature = 'sha1=' + hmac.new(b'fixture', xml, hashlib.sha1).hexdigest()
            accept_xml(xml, signature)
            self.assertEqual(push_gap_health(now=1000)['open'], 0)
