import threading
import unittest
from unittest.mock import patch

from common.protocol import Message
from server import recovery
from test_cluster import Bus


class TestReplicaWorkers(unittest.TestCase):
    def setUp(self):
        self.bus = Bus()
        self.leader, self.rm, self.other = self.bus.cluster()
        self.addCleanup(self.cancel_workers)

    def cancel_workers(self):
        for node in self.bus.nodes.values():
            with node.cv:
                node.ready = False
                node.cancel_work()

    def op(self, sequence, value, epoch=None):
        return Message('OP', epoch=epoch or self.rm.epoch,
                       op=[sequence, f'{sequence:032x}', 1, value, 'client', 4000])

    def test_two_worker_threads_apply_out_of_order_messages_in_sequence(self):
        applied = []
        process = self.rm.state.process_request

        def record(*args, **kwargs):
            result = process(*args, **kwargs)
            applied.append((threading.get_ident(), result))
            return result

        self.rm.state.process_request = record
        self.rm.handle(self.op(2, 20), (self.leader.ip, 4000))
        self.bus.wait_for(lambda: len(self.rm.replica_jobs) == 1)
        self.assertEqual(self.rm.state.num_reqs, 0)
        self.rm.handle(Message('HEARTBEAT', epoch=self.rm.epoch, probe='test'), (self.leader.ip, 4000))
        self.assertTrue(any(Message.decode(call.args[0]).type == 'ALIVE'
                            for call in self.rm.sock.sendto.call_args_list))
        self.rm.handle(self.op(1, 10), (self.leader.ip, 4000))
        self.bus.wait_for(lambda: self.rm.state.num_reqs == 2 and not self.rm.replica_jobs)
        self.assertEqual([result for _, result in applied], [('NORMAL', 1, 10), ('NORMAL', 2, 30)])
        self.assertEqual(len({identity for identity, _ in applied}), 2)
        self.assertNotIn(threading.get_ident(), {identity for identity, _ in applied})

    def test_duplicate_waiting_operation_does_not_spawn_extra_worker(self):
        for _ in range(10):
            self.rm.handle(self.op(2, 20), (self.leader.ip, 4000))
        self.assertEqual(len(self.rm.replica_jobs), 1)
        self.rm.handle(self.op(1, 10), (self.leader.ip, 4000))
        self.bus.wait_for(lambda: self.rm.state.num_reqs == 2)
        self.assertEqual(self.rm.state.total_sum, 30)

    def test_thread_start_failure_allows_operation_retry(self):
        with patch('server.replication.threading.Thread') as worker:
            worker.return_value.start.side_effect = RuntimeError('cannot start thread')
            self.rm.handle(self.op(1, 7), (self.leader.ip, 4000))
        self.assertEqual(self.rm.replica_jobs, set())
        self.assertEqual(self.rm.buffer, {})
        self.rm.handle(self.op(1, 7), (self.leader.ip, 4000))
        self.bus.wait_for(lambda: self.rm.state.num_reqs == 1)
        self.assertEqual(self.rm.state.total_sum, 7)

    def test_window_leaves_capacity_for_missing_predecessor(self):
        with patch('server.replication.MAX_INFLIGHT_OPERATIONS', 3):
            self.rm.handle(self.op(2, 2), (self.leader.ip, 4000))
            self.rm.handle(self.op(3, 3), (self.leader.ip, 4000))
            self.assertEqual(len(self.rm.replica_jobs), 2)
            self.rm.handle(self.op(1, 1), (self.leader.ip, 4000))
            self.bus.wait_for(lambda: self.rm.state.num_reqs == 3 and not self.rm.replica_jobs)
        self.assertEqual(self.rm.state.total_sum, 6)

    def test_old_result_is_cached_until_confirmed_prefix_advances(self):
        self.rm.handle(self.op(1, 10), (self.leader.ip, 4000))
        self.rm.handle(self.op(2, 20), (self.leader.ip, 4000))
        self.bus.wait_for(lambda: self.rm.state.num_reqs == 2 and not self.rm.replica_jobs)
        self.rm.sock.sendto.reset_mock()
        self.rm.handle(self.op(1, 10), (self.leader.ip, 4000))
        reply = Message.decode(self.rm.sock.sendto.call_args.args[0])
        self.assertEqual((reply.type, reply.payload['seq'], reply.payload['total']), ('RESULT', 1, 10))
        self.assertEqual(self.rm.state.total_sum, 30)
        self.rm.handle(Message('VIEW', epoch=self.rm.epoch, term=self.leader.term,
                               members=self.leader.view, seq=1), (self.leader.ip, 4000))
        self.assertNotIn(1, self.rm.result_cache)
        self.assertIn(2, self.rm.result_cache)

    def test_snapshot_cancels_waiting_old_version_worker(self):
        old_epoch = self.rm.epoch
        self.rm.handle(self.op(2, 20), (self.leader.ip, 4000))
        self.bus.wait_for(lambda: bool(self.rm.replica_jobs))
        with self.rm.cv:
            recovery.follow(self.rm, self.leader.ip, 'new-epoch', self.leader.term + 1, self.leader.view)
        self.rm.handle(Message('SNAP', epoch='new-epoch', term=self.leader.term + 1,
                               members=self.leader.view, snap=self.leader.state.snapshot()), (self.leader.ip, 4000))
        self.rm.handle(self.op(1, 5, 'new-epoch'), (self.leader.ip, 4000))
        self.bus.wait_for(lambda: self.rm.state.num_reqs == 1 and not self.rm.replica_jobs)
        self.assertEqual(self.rm.state.total_sum, 5)
        self.assertNotEqual(self.rm.epoch, old_epoch)
        self.assertEqual(self.rm.buffer, {})
