import os
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).parents[1]/'src'))
from worker_notifications import deliver_remote, retry_outbox, notify_worker_subscribers


class PersonalOutboxTests(unittest.TestCase):
    def client(self):
        client=Mock()
        client.request.side_effect=lambda path, body: {'claim':'fixture','payload':{'text':'fixture'}} if path.endswith('/claim') else {'ok':True}
        return client

    def test_remote_success_finishes_only_after_endpoint_confirmation(self):
        client=self.client()
        response=Mock(status_code=200)
        response.json.return_value={'ok':True,'queued':1,'sent':1}
        with patch('worker_notifications.requests.post',return_value=response):
            result=deliver_remote(client,'key','https://example.test','fixture')
        self.assertTrue(result['ok'])
        client.request.assert_called_with('/notifications/finish',{'key':'key','claim':'fixture','ok':True,'queued':1,'sent':1,'error':''})

    def test_server_outage_leaves_retryable_external_record(self):
        client=self.client()
        with patch('worker_notifications.requests.post',side_effect=TimeoutError):
            self.assertIsNone(deliver_remote(client,'key','https://example.test','fixture'))
        self.assertFalse(client.request.call_args.args[1]['ok'])

    def test_lost_finish_response_is_not_success(self):
        client=self.client()
        client.request.side_effect=[{'claim':'fixture','payload':{}},RuntimeError('external unavailable')]
        response=Mock(status_code=200)
        response.json.return_value={'ok':True,'sent':1}
        with patch('worker_notifications.requests.post',return_value=response):
            with self.assertRaises(RuntimeError):
                deliver_remote(client,'key','https://example.test','fixture')

    def test_explicit_unknown_is_held_only_after_coordinator_confirms(self):
        client=self.client()
        client.request.side_effect=lambda path, body: (
            {'claim':'fixture','payload':{'text':'fixture'}} if path.endswith('/claim')
            else {'ok':True,'state':'uncertain'})
        response=Mock(status_code=200)
        response.json.return_value={'ok':False,'queued':3,'sent':2,'reviewOnly':True}
        with patch('worker_notifications.requests.post',return_value=response):
            self.assertEqual(deliver_remote(client,'key','https://example.test','fixture'),
                             {'review_required':True})
        self.assertTrue(client.request.call_args.args[1]['uncertain'])
        self.assertFalse(client.request.call_args.args[1]['ok'])

    def test_older_coordinator_or_unverified_partial_remains_retryable(self):
        client=self.client()
        response=Mock(status_code=200)
        response.json.return_value={'ok':False,'queued':3,'sent':2,'reviewOnly':True}
        with patch('worker_notifications.requests.post',return_value=response):
            self.assertIsNone(deliver_remote(client,'key','https://example.test','fixture'))
        response.json.return_value={'ok':False,'queued':3,'sent':2,'reviewOnly':False}
        with patch('worker_notifications.requests.post',return_value=response):
            self.assertIsNone(deliver_remote(client,'key','https://example.test','fixture'))
        self.assertNotIn('uncertain',client.request.call_args.args[1])

    def test_review_does_not_mark_bot_only_notification_success(self):
        client=Mock()
        client.request.return_value={'state':'pending','key':'fixture'}
        with patch.dict(os.environ,{'TOPUS_PUBLISHER_OWNER':'server','TOPUS_PUBLISHER_LEASE':'fixture',
                                    'TOPUS_WORKER_URL':'https://example.test','TOPUS_WORKER_ADMIN_SECRET':'fixture'}),\
             patch('control_client.configured',return_value=True),\
             patch('control_client.ControlClient',return_value=client),\
             patch('worker_notifications.deliver_remote',return_value={'review_required':True}):
            result=notify_worker_subscribers({'code':'SciTopus'},
                {'video_id':'abcdefghijk','channel_id':'UC'+'a'*22},'fixture')
        self.assertIsNone(result)

    def test_outbox_keeps_review_visible_without_retrying_it_as_failure(self):
        client=Mock()
        client.request.return_value={'items':[{'key':'fixture'}]}
        with patch.dict(os.environ,{'TOPUS_WORKER_URL':'https://example.test','TOPUS_WORKER_ADMIN_SECRET':'fixture'}),\
             patch('control_client.configured',return_value=True),\
             patch('control_client.ControlClient',return_value=client),\
             patch('worker_notifications.deliver_remote',return_value={'review_required':True}):
            retry_outbox()
        client.heartbeat.assert_called_once_with('personal',True,'PERSONAL_DELIVERY_UNCERTAIN')


if __name__=='__main__': unittest.main()
