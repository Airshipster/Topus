import os
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).parents[1]/'src'))
from worker_notifications import deliver_remote


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


if __name__=='__main__': unittest.main()
