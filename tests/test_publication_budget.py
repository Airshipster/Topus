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
            stack.enter_context(patch('push_store.mirror_events', return_value=False))
            stack.enter_context(patch('push_store.cached_priority_science_channels', return_value=set()))
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

    def test_fresh_event_precedes_old_backlog_and_every_batch_advances(self):
        channel_id = 'UC' + 'a' * 22
        project = {'name': 'SciTopus', 'default_template': 'template', 'bot_token': 'fixture',
                   'channel_id': 'fixture'}
        events = [{'row_index': i + 2, 'video_id': f'{i:011d}', 'channel_id': channel_id,
                   'source': 'YouTube API backup', 'projects': ''} for i in range(303)]
        events.append({'row_index': 305, 'video_id': 'fresh000001', 'channel_id': channel_id,
                       'source': 'Push · server', 'projects': ''})
        processed, emitted, batch_sizes = set(), [], []

        def get_events(_sheet):
            result = [event for event in events if event['row_index'] not in processed]
            batch_sizes.append(len(result))
            return result

        def mark(_sheet, entries):
            processed.update(entry['row_index'] for entry in entries)

        def metadata(ids):
            return {video_id: ({'title': video_id, 'channel': 'fixture',
                                'published': 'new' if video_id == 'fresh000001' else 'old'}, None)
                    for video_id in ids}

        funcs = {
            'authenticate_google_sheets': Mock(), 'open_master_sheet_with_retry': Mock(),
            'acquire_lock_with_wait': Mock(return_value=True), 'release_lock': Mock(),
            'update_run_status': Mock(), 'load_settings': Mock(return_value={}),
            'apply_runtime_overrides': Mock(), 'print_detection_latency_note': Mock(),
            'load_projects': Mock(return_value=[project]), 'get_push_events': get_events,
            'pending_events': Mock(return_value=[]), 'select_push_projects': Mock(return_value=[project]),
            'load_project_channels': Mock(return_value=({'SciTopus': {channel_id: {}}}, {})),
            'get_published_videos': lambda *_: {(video_id, 'SciTopus') for video_id in emitted},
            'pending_hold_reason': Mock(return_value=''),
            'get_stale_reason': lambda date, *_: 'old' if date == 'old' else '',
            'should_filter_video': Mock(return_value=(False, '')),
            'save_videos_batch': lambda _sheet, videos: [(v['video_id'], p['name']) for v, p, *_ in videos],
            'get_videos_info_from_api': metadata, 'mark_push_events_processed_batch': mark,
            'format_message': Mock(return_value='fixture'),
            'send_public': lambda _token, _channel, _message, _project, video_id, **_kw:
                (emitted.append(video_id) or '1'),
            'notify_worker_subscribers': Mock(return_value=None),
            'update_video_publication_status_batch': lambda _sheet, entries: len(entries),
            'log_events_batch': Mock(),
            'get_youtube_api_calls': Mock(return_value=0), 'update_last_run': Mock(),
        }
        with ExitStack() as stack:
            stack.enter_context(patch.dict('os.environ', {'TOPUS_PUBLISHER_OWNER': 'server',
                                                         'TOPUS_PUSH_ONLY': 'true'}, clear=True))
            stack.enter_context(patch('control_client.configured', return_value=False))
            stack.enter_context(patch('push_store.mirror_events', return_value=False))
            stack.enter_context(patch('push_store.cached_priority_science_channels', return_value={channel_id}))
            stack.enter_context(patch('sheets.close_deferred_videos_batch'))
            stack.enter_context(patch('activity_retention.retention_due', return_value=False))
            stack.enter_context(patch('time.sleep'))
            stack.enter_context(patch('builtins.print'))
            for name, value in funcs.items():
                stack.enter_context(patch.object(main, name, value))
            results = [main.main() for _ in range(4)]
        self.assertEqual(results, [CONTINUE_QUEUE, CONTINUE_QUEUE, CONTINUE_QUEUE, None])
        self.assertEqual(batch_sizes, [304, 204, 104, 4])
        self.assertEqual(emitted, ['fresh000001'])
        self.assertEqual(len(processed), 304)

    def test_unavailable_metadata_does_not_cause_tight_retry_loop(self):
        channel_id = 'UC' + 'a' * 22
        project = {'name': 'SciTopus', 'default_template': 'template'}
        events = [{'row_index': i + 2, 'video_id': f'{i:011d}', 'channel_id': channel_id,
                   'source': 'Push · server', 'projects': ''} for i in range(101)]
        funcs = {
            'authenticate_google_sheets': Mock(), 'open_master_sheet_with_retry': Mock(),
            'acquire_lock_with_wait': Mock(return_value=True), 'release_lock': Mock(),
            'update_run_status': Mock(), 'load_settings': Mock(return_value={}),
            'apply_runtime_overrides': Mock(), 'print_detection_latency_note': Mock(),
            'load_projects': Mock(return_value=[project]), 'get_push_events': Mock(return_value=events),
            'pending_events': Mock(return_value=[]), 'select_push_projects': Mock(return_value=[project]),
            'load_project_channels': Mock(return_value=({'SciTopus': {channel_id: {}}}, {})),
            'get_published_videos': Mock(return_value=set()),
            'get_videos_info_from_api': lambda ids: {i: (None, 'quotaExceeded') for i in ids},
            'save_videos_batch': Mock(return_value=[]), 'mark_push_events_processed_batch': Mock(),
            'log_events_batch': Mock(), 'update_video_publication_status_batch': Mock(),
            'get_youtube_api_calls': Mock(return_value=0), 'update_last_run': Mock(),
        }
        with ExitStack() as stack:
            stack.enter_context(patch.dict('os.environ', {'TOPUS_PUBLISHER_OWNER': 'server',
                                                         'TOPUS_PUSH_ONLY': 'true'}, clear=True))
            stack.enter_context(patch('control_client.configured', return_value=False))
            stack.enter_context(patch('push_store.mirror_events', return_value=False))
            stack.enter_context(patch('push_store.cached_priority_science_channels', return_value=set()))
            stack.enter_context(patch('sheets.close_deferred_videos_batch'))
            stack.enter_context(patch('activity_retention.retention_due', return_value=False))
            stack.enter_context(patch('builtins.print'))
            for name, value in funcs.items():
                stack.enter_context(patch.object(main, name, value))
            with self.assertRaisesRegex(RuntimeError, 'Incomplete pass'):
                main.main()
        funcs['mark_push_events_processed_batch'].assert_not_called()


if __name__ == '__main__':
    unittest.main()
