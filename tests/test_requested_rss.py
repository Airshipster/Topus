import io
import unittest
from unittest.mock import Mock, patch
import controller

class RequestedRssTests(unittest.TestCase):
    def request(self, body, auth='Bearer fixture'):
        request = Mock(path='/run', headers={'Content-Length':str(len(body)), 'Authorization':auth},
                       rfile=io.BytesIO(body))
        with patch.object(controller, 'TOKEN', 'fixture'):
            controller.Handler.do_POST(request)
        return request

    def setUp(self):
        controller.requested_rss.clear()
        controller.requested_hot_rss.clear()
        controller.wake.clear()

    def test_authenticated_request_queues_without_running(self):
        with patch.object(controller, 'run_job') as run:
            self.request(b'{"mode":"rss"}').reply.assert_called_with(202, {'accepted':True,'owner':'server'})
            self.assertTrue(controller.requested_rss.is_set())
            run.assert_not_called()

    def test_invalid_and_unauthorized_requests_cannot_queue(self):
        self.request(b'{"mode":"rss"}', 'wrong').reply.assert_called_with(401, {'error':'unauthorized'})
        self.request(b'{"mode":"other"}').reply.assert_called_with(400, {'error':'invalid_request'})
        self.assertFalse(controller.requested_rss.is_set())

    def test_hot_rss_contention_schedules_short_retry(self):
        timer = Mock()
        factory = Mock(return_value=timer)
        self.assertTrue(controller.schedule_hot_retry(75, timer_factory=factory))
        factory.assert_called_once_with(45, controller.requested_hot_rss.set)
        self.assertTrue(timer.daemon)
        timer.start.assert_called_once_with()

    def test_hot_rss_success_does_not_schedule_retry(self):
        factory = Mock()
        self.assertFalse(controller.schedule_hot_retry(0, timer_factory=factory))
        factory.assert_not_called()
