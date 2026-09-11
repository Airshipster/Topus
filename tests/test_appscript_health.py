import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).parents[1]/'src'))
from appscript_health import valid_health


class AppScriptHealthTests(unittest.TestCase):
    def test_requires_executed_fresh_queue_read(self):
        valid={'kind':'topus-push-queue-read','ok':True,'checkedAt':1000000}
        self.assertTrue(valid_health(valid,1005))
        self.assertFalse(valid_health(valid,1400))
        self.assertFalse(valid_health({**valid,'ok':False},1005))
        self.assertFalse(valid_health({'ok':True},1005))
