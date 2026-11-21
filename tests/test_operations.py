import unittest

from server.operations import PendingOperation


class TestPendingOperation(unittest.TestCase):
    def operation(self, sequence, total):
        return PendingOperation('epoch', sequence, 'a' * 32, 1, 7,
                                ('client', 4000), total, frozenset({'rm1', 'rm2'}))

    def test_confirmations_are_independent_for_each_sequence(self):
        first, second = self.operation(1, 7), self.operation(2, 14)
        self.assertTrue(second.acknowledge('rm1', 'epoch', 2, 14, {'rm1', 'rm2'}))
        self.assertFalse(first.acknowledge('rm1', 'epoch', 2, 14, {'rm1', 'rm2'}))
        self.assertEqual(first.acks, set())
        self.assertEqual(second.acks, {'rm1'})

    def test_all_active_participants_are_required_and_newcomers_are_not_added(self):
        operation = self.operation(1, 7)
        operation.acks.add('rm1')
        self.assertFalse(operation.confirmed({'rm1', 'rm2'}))
        self.assertTrue(operation.confirmed({'rm1', 'newcomer'}))
        self.assertFalse(operation.acknowledge('newcomer', 'epoch', 1, 7, {'newcomer'}))

    def test_invalid_or_cancelled_confirmation_is_ignored(self):
        operation = self.operation(1, 7)
        for epoch, total in (('old', 7), ('epoch', 99)):
            self.assertFalse(operation.acknowledge('rm1', epoch, 1, total, {'rm1'}))
        operation.cancelled = True
        self.assertFalse(operation.acknowledge('rm1', 'epoch', 1, 7, {'rm1'}))
