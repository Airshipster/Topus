import sys
from pathlib import Path
import unittest
import io
import json
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[1]/'src'))
from appscript_health import valid_health, stale_component, main


class AppScriptHealthTests(unittest.TestCase):
    def test_full_monitor_recovers_stale_cron_without_repeating_fresh_checks(self):
        for age, expected in [(60, True), (0, False)]:
            with self.subTest(age=age):
                calls = []
                class Response(io.BytesIO):
                    status = 200
                class Opener:
                    def open(self, request, timeout):
                        path = request.full_url.split('.workers.dev')[1]
                        calls.append(path)
                        body = {'beats':{'server-http':{'success_minutes':age},
                            'appscript':{'error':'','seen_minutes':0}}} if path == '/status' else {'ok':True}
                        return Response(json.dumps(body).encode())
                with patch.dict('os.environ', {'TOPUS_CONTROL_TOKEN':'fixture',
                    'TOPUS_CONTROL_URL':'https://topus-publication-control.scitopus.workers.dev'}), \
                    patch('appscript_health.urllib.request.build_opener',return_value=Opener()), \
                    patch('appscript_health.urllib.request.urlopen',return_value=Response(b'{}')), \
                    patch('builtins.print'):
                    main()
                self.assertEqual('/monitor' in calls, expected)

    def test_heartbeat_contact_does_not_hide_failed_work(self):
        self.assertTrue(stale_component({'success_minutes':60,'seen_minutes':0},45))
        self.assertFalse(stale_component({'success_minutes':2,'seen_minutes':0},45))
        self.assertFalse(stale_component({},45))

    def test_requires_executed_fresh_queue_read(self):
        valid={'kind':'topus-push-queue-read','ok':True,'checkedAt':1000000}
        self.assertTrue(valid_health(valid,1005))
        self.assertFalse(valid_health(valid,1400))
        self.assertFalse(valid_health({**valid,'ok':False},1005))
        self.assertFalse(valid_health({'ok':True},1005))
