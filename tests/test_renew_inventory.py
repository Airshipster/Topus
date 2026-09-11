import os
import tempfile
import unittest
from unittest.mock import patch

from push_store import database
from renew_direct import reconcile_inventory


class RenewalInventoryTest(unittest.TestCase):
    def test_partial_inventory_preserves_other_projects(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'TOPUS_PUSH_DB': directory + '/push.db'}):
            reconcile_inventory({'old': {}}, True)
            reconcile_inventory({'new': {}}, False)
            with database() as db:
                self.assertEqual(dict(db.execute('SELECT channel_id,enabled FROM leases')), {'old': 1, 'new': 1})
            reconcile_inventory({'new': {}}, True)
            with database() as db:
                self.assertEqual(dict(db.execute('SELECT channel_id,enabled FROM leases')), {'old': 0, 'new': 1})

    def test_empty_inventory_never_disables_existing(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'TOPUS_PUSH_DB': directory + '/push.db'}):
            reconcile_inventory({'old': {}}, True)
            with self.assertRaises(RuntimeError):
                reconcile_inventory({}, True)
            with database() as db:
                self.assertEqual(db.execute('SELECT enabled FROM leases').fetchone()[0], 1)
