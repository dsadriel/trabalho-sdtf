import unittest

from server.operations import PendingOperation
from server.quorum import required


class TestPendingOperation(unittest.TestCase):
    def operation(self, sequence, total, members=frozenset({'rm1', 'rm2'})):
        return PendingOperation('epoch', sequence, 'a' * 32, 1, 7,
                                ('client', 4000), total, members)

    def test_required_quorum_uses_majority_with_minimum_one_replica(self):
        self.assertEqual([required(n) for n in range(6)], [1, 1, 2, 2, 3, 3])

    def test_confirmations_are_independent_for_each_sequence(self):
        first, second = self.operation(1, 7), self.operation(2, 14)
        self.assertTrue(second.acknowledge('rm1', 'epoch', 2, 14, {'rm1', 'rm2'}))
        self.assertFalse(first.acknowledge('rm1', 'epoch', 2, 14, {'rm1', 'rm2'}))
        self.assertEqual(first.acks, set())
        self.assertEqual(second.acks, {'rm1'})

    def test_majority_of_active_participants_is_required_and_newcomers_are_ignored(self):
        operation = self.operation(1, 7, members=frozenset({'rm1', 'rm2', 'rm3'}))
        operation.acks.add('rm1')
        self.assertFalse(operation.confirmed({'rm1', 'rm2', 'rm3'}))
        operation.acks.add('rm2')
        self.assertTrue(operation.confirmed({'rm1', 'rm2', 'rm3'}))
        self.assertFalse(operation.confirmed({'newcomer'}))
        self.assertFalse(operation.acknowledge('newcomer', 'epoch', 1, 7, {'newcomer'}))

    def test_two_active_participants_require_both_and_zero_blocks(self):
        operation = self.operation(1, 7)
        operation.acks.add('rm1')
        self.assertFalse(operation.confirmed({'rm1', 'rm2'}))
        self.assertTrue(operation.confirmed({'rm1', 'newcomer'}))
        self.assertFalse(operation.confirmed(set()))

    def test_invalid_or_cancelled_confirmation_is_ignored(self):
        operation = self.operation(1, 7)
        for epoch, total in (('old', 7), ('epoch', 99)):
            self.assertFalse(operation.acknowledge('rm1', epoch, 1, total, {'rm1'}))
        operation.cancelled = True
        self.assertFalse(operation.acknowledge('rm1', 'epoch', 1, 7, {'rm1'}))
