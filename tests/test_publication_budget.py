import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
from publication_budget import CONTINUE_QUEUE, PublicationBudget
from retry_queue import pending_events
import main


class PublicationBudgetTests(unittest.TestCase):
    def test_yields_only_between_attempts_and_allows_initial_progress(self):
        now = [100.0]
        budget = PublicationBudget(clock=lambda: now[0], seconds=600)
        now[0] = 1000.0
        self.assertFalse(budget.should_yield())
        budget.attempted()
        self.assertTrue(budget.should_yield())

    def test_long_durable_queue_continues_without_repeating_confirmed_items(self):
        channel = 'https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa'
        headers = ['Проект', 'Ссылка на видео', 'Ссылка на канал', 'Системный статус', 'TG message_id']
        rows = [['SciTopus', f'https://www.youtube.com/watch?v={index:011d}',
                 channel, 'Push: pending', ''] for index in range(25)]
        sent = []
        passes = 0
        while True:
            with patch('retry_queue.get_values_with_quota_retry', return_value=[headers] + rows):
                events = pending_events(Mock())
            if not events:
                break
            now = [0]
            budget = PublicationBudget(clock=lambda: now[0], seconds=600)
            for event in events:
                if budget.should_yield():
                    break
                budget.attempted()
                item = next(row for row in rows if row[1].endswith(event['video_id']))
                sent.append(event['video_id'])
                item[3], item[4] = 'Push: published', str(len(sent))
                now[0] += 61
            passes += 1
            self.assertLess(passes, 10)
        self.assertEqual(CONTINUE_QUEUE, 76)
        self.assertEqual(passes, 3)
        self.assertEqual(len(sent), 25)
        self.assertEqual(len(set(sent)), 25)

    def test_publisher_flushes_each_batch_and_recovers_pending_rows(self):
        channel_id = 'UC' + 'a' * 22
        headers = ['Проект', 'Ссылка на видео', 'Ссылка на канал', 'Системный статус', 'TG message_id']
        rows = [['SciTopus', f'https://www.youtube.com/watch?v={index:011d}',
                 f'https://youtube.com/channel/{channel_id}', 'Push: pending', ''] for index in range(25)]
        project = {'name': 'SciTopus', 'default_template': 'template', 'bot_token': 'fixture',
                   'channel_id': 'fixture'}
        now = [0]
        receipts = {'00000000000': 'existing'}
        emitted = []

        def update(_sheet, entries):
            for entry in entries:
                row = next(row for row in rows if row[1].endswith(entry['video_id']))
                row[3], row[4] = entry['status'], str(entry.get('tg_message_id') or '')
            return len(entries)

        def send(_token, _channel, _message, _project, video_id, **_kwargs):
            if video_id not in receipts:
                receipts[video_id] = str(len(receipts) + 1)
                emitted.append((video_id, now[0]))
                now[0] += 61
            return receipts[video_id]

        funcs = {
            'authenticate_google_sheets': Mock(), 'open_master_sheet_with_retry': Mock(),
            'acquire_lock_with_wait': Mock(return_value=True), 'release_lock': Mock(),
            'update_run_status': Mock(), 'load_settings': Mock(return_value={}),
            'apply_runtime_overrides': Mock(), 'print_detection_latency_note': Mock(),
            'load_projects': Mock(return_value=[project]), 'get_push_events': lambda *_: [],
            'select_push_projects': Mock(return_value=[project]),
            'load_project_channels': Mock(return_value=({'SciTopus': {channel_id: {}}}, {})),
            'get_published_videos': lambda *_: set(), 'pending_hold_reason': Mock(return_value=''),
            'get_stale_reason': Mock(return_value=''), 'should_filter_video': Mock(return_value=(False, '')),
            'save_videos_batch': lambda _sheet, videos: [(v['video_id'], p['name']) for v, p, *_ in videos],
            'get_videos_info_from_api': lambda ids: {i: ({'title': i, 'channel': 'fixture',
                                                          'published': '2026-11-08T00:00:00Z'}, None) for i in ids},
            'format_message': Mock(return_value='fixture'), 'send_public': send,
            'notify_worker_subscribers': Mock(return_value=None),
            'update_video_publication_status_batch': update, 'log_events_batch': Mock(),
            'get_youtube_api_calls': Mock(return_value=0), 'update_last_run': Mock(),
            'PublicationBudget': lambda: PublicationBudget(clock=lambda: now[0], seconds=600),
        }
        with ExitStack() as stack:
            stack.enter_context(patch.dict('os.environ', {'TOPUS_PUBLISHER_OWNER': 'server',
                                                         'TOPUS_PUSH_ONLY': 'true'}, clear=True))
            stack.enter_context(patch('control_client.configured', return_value=False))
            stack.enter_context(patch('push_store.mirror_events'))
            stack.enter_context(patch('sheets.close_deferred_videos_batch'))
            stack.enter_context(patch('retry_queue.get_values_with_quota_retry', side_effect=lambda *_: [headers] + rows))
            stack.enter_context(patch('activity_retention.retention_due', return_value=False))
            stack.enter_context(patch('time.sleep'))
            for name, value in funcs.items():
                stack.enter_context(patch.object(main, name, value))
            results = [main.main(), main.main(), main.main()]

        self.assertEqual(results, [CONTINUE_QUEUE, CONTINUE_QUEUE, None])
        self.assertEqual(len(emitted), 24)
        self.assertEqual(len(receipts), 25)
        self.assertTrue(all(row[3] == 'published' and row[4] for row in rows))
        self.assertTrue(all(b[1] - a[1] >= 60 for a, b in zip(emitted, emitted[1:])))


if __name__ == '__main__':
    unittest.main()
