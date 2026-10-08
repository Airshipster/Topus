import unittest
from unittest.mock import patch

from renew_direct import TOPIC_PATH, select_renewal_batch


class RenewalPriorityTests(unittest.TestCase):
    now = 200000

    def lease(self, channel, **changes):
        return {'channel_id': channel, 'topic_path': TOPIC_PATH, 'expires': self.now + 50000,
                'requested': 50000, 'verified': 50010, **changes}

    def select(self, leases, channels, limit=10):
        with patch.dict('os.environ', {'TOPUS_PRIORITY_PROJECTS': 'SciTopus'}):
            return [item['channel_id'] for item in select_renewal_batch(leases, channels, self.now, limit)]

    def test_scitopus_and_shared_channels_precede_other_projects(self):
        leases = [self.lease(name) for name in ('a-other', 'z-science', 'shared')]
        channels = {'a-other': {'projects': ['Other']}, 'z-science': {'projects': [' scitopus ']},
                    'shared': {'projects': ['Other', 'SciTopus']}}
        self.assertEqual(self.select(leases, channels), ['shared', 'z-science', 'a-other'])

    def test_unknown_inventory_cannot_consume_the_batch_limit(self):
        leases = [self.lease('unknown-' + str(n)) for n in range(20)] + [self.lease('science')]
        self.assertEqual(self.select(leases, {'science': {'projects': ['SciTopus']}}, 1), ['science'])

    def test_unconfirmed_science_retry_does_not_starve_other_first_attempts(self):
        leases = [self.lease('science', requested=self.now - 400), self.lease('other')]
        channels = {'science': {'projects': ['SciTopus']}, 'other': {'projects': ['Other']}}
        self.assertEqual(self.select(leases, channels, 1), ['other'])

    def test_retries_rotate_oldest_first_instead_of_repeated_priority(self):
        leases = [self.lease('science', requested=self.now - 400),
                  self.lease('other', requested=self.now - 500)]
        channels = {'science': {'projects': ['SciTopus']}, 'other': {'projects': ['Other']}}
        self.assertEqual(self.select(leases, channels), ['other', 'science'])

    def test_same_age_retries_prioritize_science(self):
        leases = [self.lease(name, requested=self.now - 400) for name in ('other', 'science')]
        channels = {'science': {'projects': ['SciTopus']}, 'other': {'projects': ['Other']}}
        self.assertEqual(self.select(leases, channels), ['science', 'other'])

    def test_limits_and_cooldowns_remain_in_force(self):
        leases = [self.lease('not-due', expires=self.now + 100000),
                  self.lease('cooldown', requested=self.now - 100),
                  self.lease('disabled', enabled=0), self.lease('ready')]
        channels = {lease['channel_id']: {'projects': ['SciTopus']} for lease in leases}
        self.assertEqual(self.select(leases, channels), ['ready'])
        self.assertEqual(self.select(leases, channels, 0), [])

    def test_new_channel_and_topic_migration_are_first_attempts(self):
        leases = [self.lease('new', requested=0, verified=0, expires=0, topic_path=''),
                  self.lease('migration', expires=self.now + 200000, topic_path='/xml/feeds/videos.xml')]
        channels = {'new': {'projects': ['SciTopus']}, 'migration': {'projects': ['Other']}}
        self.assertEqual(self.select(leases, channels), ['new', 'migration'])
