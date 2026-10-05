import unittest
from unittest.mock import Mock, patch
import controller


class DiscoverySchedulerTests(unittest.TestCase):
    def test_full_inventory_is_not_scanned_every_five_minutes(self):
        self.assertEqual(controller.next_discovery_job(100, 100, 400), 'rss-hot-discovery')
        self.assertIsNone(controller.next_discovery_job(100, 400, 401))
        self.assertEqual(controller.next_discovery_job(100, 1600, 1899), None)
        self.assertEqual(controller.next_discovery_job(100, 1600, 1900), 'rss-discovery')

    def test_cold_start_and_full_scan_take_priority(self):
        self.assertEqual(controller.next_discovery_job(None, None, 100), 'rss-discovery')
        self.assertEqual(controller.next_discovery_job(100, None, 101), 'rss-hot-discovery')
        self.assertEqual(controller.next_discovery_job(100, 100, 1900), 'rss-discovery')

    def test_empty_scan_does_not_preempt_publication(self):
        with patch.object(controller, 'database') as database, patch.object(controller, 'wake') as wake:
            database.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
            controller.wake_pending_discoveries()
            wake.set.assert_not_called()

    def test_new_persisted_event_wakes_publisher(self):
        with patch.object(controller, 'database') as database, patch.object(controller, 'wake') as wake:
            database.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (1,)
            controller.wake_pending_discoveries()
            wake.set.assert_called_once_with()

    def test_successful_full_discovery_reports_actual_rss_health(self):
        child = Mock()
        child.poll.return_value = 0
        child.returncode = 0
        with patch.object(controller, 'database'), patch.object(controller.subprocess, 'Popen', return_value=child), \
                patch.object(controller.Path, 'read_text', return_value='{}'), \
                patch.dict(controller.os.environ, {'GOOGLE_SERVICE_ACCOUNT_JSON_FILE': '/fixture.json'}), \
                patch.object(controller, 'configured', return_value=True), patch.object(controller, 'ControlClient') as client:
            self.assertEqual(controller.run_job('rss-discovery'), 0)
            client.return_value.heartbeat.assert_called_once_with('rss', True, '')

    def test_failed_full_discovery_does_not_claim_success(self):
        child = Mock()
        child.poll.return_value = 1
        child.returncode = 1
        with patch.object(controller, 'database'), patch.object(controller.subprocess, 'Popen', return_value=child), \
                patch.object(controller.Path, 'read_text', return_value='{}'), \
                patch.dict(controller.os.environ, {'GOOGLE_SERVICE_ACCOUNT_JSON_FILE': '/fixture.json'}), \
                patch.object(controller, 'configured', return_value=True), patch.object(controller, 'ControlClient') as client:
            self.assertEqual(controller.run_job('rss-discovery'), 1)
            client.return_value.heartbeat.assert_called_once_with('rss', False, 'exit 1')


if __name__ == '__main__':
    unittest.main()
