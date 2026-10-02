import os
import tempfile
import unittest
from unittest.mock import patch

from push_store import database
from renew_direct import reconcile_inventory, hub_backoff, hub_ready, renewal_pause, topic_for_renewal, TOPIC_PATH


class RenewalInventoryTest(unittest.TestCase):
    def test_expiring_verified_legacy_lease_is_preserved_during_provider_outage(self):
        self.assertEqual(topic_for_renewal({'topic_path':'/xml/feeds/videos.xml','expires':1100},now=1000),
                         '/xml/feeds/videos.xml')
        self.assertEqual(topic_for_renewal({'topic_path':'/xml/feeds/videos.xml','expires':100000},now=1000),
                         TOPIC_PATH)
        self.assertEqual(topic_for_renewal({'topic_path':'','expires':0},now=1000),TOPIC_PATH)
        self.assertEqual(topic_for_renewal({'topic_path':TOPIC_PATH,'expires':1100},now=1000),TOPIC_PATH)

    def test_official_topic_and_bounded_migration_continuation(self):
        self.assertEqual(TOPIC_PATH, '/feeds/videos.xml')
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'TOPUS_PUSH_DB': directory + '/push.db'}):
            self.assertEqual(renewal_pause(now=1000), 120)
            with database() as db:
                db.execute("INSERT INTO leases(channel_id,topic_path) VALUES ('active','/xml/feeds/videos.xml')")
                db.execute("INSERT INTO leases(channel_id,enabled) VALUES ('disabled',0)")
            self.assertEqual(renewal_pause(now=1000), 5)
            hub_backoff(503, '600', now=1000)
            self.assertEqual(renewal_pause(now=1000), 120)
            with database() as db:
                db.execute('UPDATE leases SET topic_path=? WHERE channel_id=?', (TOPIC_PATH, 'active'))
            self.assertEqual(renewal_pause(now=1601), 120)

    def test_rate_limit_backoff_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'TOPUS_PUSH_DB': directory + '/push.db'}):
            hub_backoff(429, '900', now=100)
            self.assertFalse(hub_ready(now=999))
            self.assertTrue(hub_ready(now=1000))
            hub_backoff(503, '', now=1000)
            self.assertFalse(hub_ready(now=1119))
            self.assertTrue(hub_ready(now=1120))

    def test_success_does_not_pause_hub(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'TOPUS_PUSH_DB': directory + '/push.db'}):
            hub_backoff(202, now=100)
            self.assertTrue(hub_ready(now=100))

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
