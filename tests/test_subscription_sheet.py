import unittest
from subscription_sheet import obsolete_rows


class SubscriptionSheetTest(unittest.TestCase):
    def test_only_confirmed_disabled_is_removed(self):
        values = [['Channel ID'], ['active'], ['disabled'], ['unknown'], []]
        leases = {'active': {'enabled': 1}, 'disabled': {'enabled': 0}}
        self.assertEqual(obsolete_rows(values, leases, {'active': {}}, True), [3])

    def test_partial_and_empty_inventory_preserves_all(self):
        values = [['Channel ID'], ['disabled']]
        leases = {'disabled': {'enabled': 0}}
        self.assertEqual(obsolete_rows(values, leases, {'active': {}}, False), [])
        self.assertEqual(obsolete_rows(values, leases, {}, True), [])

    def test_reenabled_channel_is_preserved(self):
        self.assertEqual(obsolete_rows([['Channel ID'], ['old']],
                         {'old': {'enabled': 0}}, {'old': {}}, True), [])
