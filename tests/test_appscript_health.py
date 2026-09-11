import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).parents[1]/'src'))
from appscript_health import valid_health, stale_component


class AppScriptHealthTests(unittest.TestCase):
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
