import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
from control_client import ControlClient, ControlUnavailable
from coordinated_run import run
import delivery_journal
import push_store


class CoordinationTests(unittest.TestCase):
    def test_coordinated_owner_does_not_wait_on_or_mutate_legacy_lock(self):
        from main import acquire_lock_with_wait
        sheet = Mock()
        with patch('control_client.configured', return_value=True), \
             patch('control_client.ControlClient') as client, \
             patch.dict(os.environ, {'TOPUS_PUBLISHER_OWNER':'server', 'TOPUS_PUBLISHER_LEASE':'fixture'}):
            client.return_value.request.return_value = {'ok':True}
            self.assertTrue(acquire_lock_with_wait(sheet))
            sheet.assert_not_called()
            self.assertEqual(sheet.mock_calls, [])
            client.return_value.request.return_value = {'ok':False}
            with self.assertRaisesRegex(RuntimeError, 'lease lost'):
                acquire_lock_with_wait(sheet)

    def test_config_fails_closed(self):
        for url in ['', 'http://example.test', 'https://user:pass@example.test']:
            with self.assertRaises(ControlUnavailable):
                ControlClient(url=url, token='fixture')

    def test_no_local_send_when_control_missing_or_fails(self):
        with patch.dict(os.environ, {'TOPUS_CONTROL_REQUIRED': 'true', 'TOPUS_CONTROL_URL': ''}), \
             patch('requests.post') as direct:
            with self.assertRaises(ControlUnavailable):
                delivery_journal.send_public('secret', 'channel', 'text', 'project', 'abcdefghijk')
            direct.assert_not_called()

    def test_request_does_not_follow_redirect_or_leak_secret(self):
        session = Mock()
        session.request.side_effect = RuntimeError('token=fixture')
        client = ControlClient('https://example.test', 'fixture', session)
        with self.assertRaisesRegex(ControlUnavailable, '^CONTROL_UNAVAILABLE$'):
            client.request('/status')
        self.assertFalse(session.request.call_args.kwargs['allow_redirects'])
        self.assertEqual(session.request.call_args.kwargs['timeout'], (5, 20))

    def test_standby_does_not_spawn_or_report_rss_success(self):
        client = Mock()
        client.request.return_value = {'token': None}
        spawn = Mock()
        with patch.dict(os.environ, {'TOPUS_PUBLISHER_OWNER': 'github'}):
            self.assertEqual(run(client, spawn), 75)
        spawn.assert_not_called()
        self.assertNotIn(unittest.mock.call('rss', True), client.heartbeat.call_args_list)

    def test_success_sets_lease_and_heartbeat(self):
        client = Mock()
        client.request.return_value = {'token': 'fixture-lease'}
        child = Mock()
        child.wait.return_value = 0
        spawn = Mock(return_value=child)
        with patch.dict(os.environ, {'TOPUS_PUBLISHER_OWNER': 'server', 'TOPUS_PUSH_ONLY': 'false'}):
            self.assertEqual(run(client, spawn), 0)
        self.assertEqual(spawn.call_args.kwargs['env']['TOPUS_PUBLISHER_LEASE'], 'fixture-lease')
        client.heartbeat.assert_any_call('server-publisher', True, '')
        client.heartbeat.assert_any_call('rss', True)
        client.request.assert_any_call('/lease/release', {'owner': 'server', 'lease': 'fixture-lease'})

    def test_failed_renewal_stops_child_and_does_not_claim_success(self):
        client = Mock()
        def request(path, body=None):
            if path == '/lease/acquire':
                return {'token': 'fixture-lease'}
            if path == '/lease/renew':
                raise ControlUnavailable('CONTROL_UNAVAILABLE')
            return {'ok': True}
        client.request.side_effect = request
        child = Mock()
        child.wait.side_effect = [subprocess.TimeoutExpired('fixture', 20), 0]
        with patch.dict(os.environ, {'TOPUS_PUBLISHER_OWNER': 'server'}), \
             patch('coordinated_run.stop_child') as stop:
            with self.assertRaises(ControlUnavailable):
                run(client, Mock(return_value=child), clock=lambda: 0)
        stop.assert_called_once_with(child)
        self.assertNotIn(unittest.mock.call('server-publisher', True, ''), client.heartbeat.call_args_list)

    def test_auto_mode_preserves_rss_cadence(self):
        client = Mock()
        client.request.side_effect = lambda path, body=None: (
            {'token': 'lease'} if path == '/lease/acquire' else
            {'beats': {'rss': {'success_minutes': 10}}} if path == '/status' else {'ok': True})
        child = Mock()
        child.wait.return_value = 0
        spawn = Mock(return_value=child)
        with patch.dict(os.environ, {'TOPUS_PUBLISHER_OWNER': 'github', 'TOPUS_PUSH_ONLY': 'auto'}):
            self.assertEqual(run(client, spawn), 0)
        self.assertEqual(spawn.call_args.kwargs['env']['TOPUS_PUSH_ONLY'], 'true')
        self.assertNotIn(unittest.mock.call('rss', True), client.heartbeat.call_args_list)

    def test_disabled_coordinator_never_starts_publisher(self):
        client = Mock()
        client.request.return_value = {'active': False}
        spawn = Mock()
        with patch.dict(os.environ, {'TOPUS_PUBLISHER_OWNER': 'github'}):
            self.assertEqual(run(client, spawn), 75)
        spawn.assert_not_called()
        self.assertNotIn(unittest.mock.call('rss', True), client.heartbeat.call_args_list)

    def test_remote_queue_drains_without_local_ingress(self):
        self.check_remote_mirror(fail_append=False)

    def test_sheet_failure_does_not_ack_remote_event(self):
        self.check_remote_mirror(fail_append=True)

    def check_remote_mirror(self, fail_append):
        client = Mock()
        event = {'key': 'fixture-event', 'video_id': 'abcdefghijk',
                 'channel_id': 'UC' + 'a' * 22, 'received': 1789000000,
                 'source': 'RSS · server'}
        client.request.side_effect = lambda path, body=None: {'events': [event]} if path == '/events/pending' else {'ok': True}
        worksheet = Mock()
        worksheet.get_all_values.return_value = [['Timestamp GMT+4', 'Video ID', 'Ссылка на канал', 'Обработано', 'Проекты', 'Источник']]
        if fail_append:
            worksheet.append_rows.side_effect = RuntimeError('fixture write failure')
        sheet = Mock()
        sheet.worksheet.return_value = worksheet
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
            'TOPUS_PUSH_DB': str(Path(directory) / 'push.sqlite3'), 'TOPUS_CONTROL_REQUIRED': 'true',
            'TOPUS_PUBLISHER_OWNER': 'github', 'TOPUS_PUBLISHER_LEASE': 'fixture-lease',
        }), patch('control_client.ControlClient', return_value=client):
            if fail_append:
                with self.assertRaises(RuntimeError):
                    push_store.mirror_events(sheet)
            else:
                push_store.mirror_events(sheet)
        acknowledged = [call for call in client.request.call_args_list if call.args[0] == '/events/ack']
        self.assertEqual(len(acknowledged), 0 if fail_append else 1)
        worksheet.append_rows.assert_called_once()
        self.assertEqual(worksheet.append_rows.call_args.args[0][0][-1], 'RSS · server')


if __name__ == '__main__':
    unittest.main()
