import time
import unittest
from unittest.mock import patch

from common.protocol import Message
from test_cluster import Bus


class TestConcurrency(unittest.TestCase):
    def setUp(self):
        self.bus = Bus()
        self.leader, self.first, self.second = self.bus.cluster()
        self.original_nodes = list(self.bus.nodes.values())
        self.addCleanup(self.cancel_workers)

    def cancel_workers(self):
        for node in set(self.original_nodes) | set(self.bus.nodes.values()):
            with node.cv:
                node.ready = False
                node.cancel_work()

    def wait(self, predicate):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            self.bus.drain()
            if predicate():
                return
            time.sleep(0.001)
        self.fail('Concurrent protocol did not reach the expected state')

    def request(self, client, value=7, request_id=1):
        session = f'{client:032x}'
        self.leader.handle(Message.request(request_id, value, session=session), ('client', 4000))

    def hold_results(self, sequence=None):
        held = []
        deliveries = {}
        for node in (self.first, self.second):
            deliver = node.sock.sendto.side_effect
            deliveries[node.ip] = deliver

            def send(data, address, node=node, deliver=deliver):
                message = Message.decode(data)
                if message.type == 'RESULT' and (sequence is None or message.payload['seq'] == sequence):
                    held.append((node.ip, data, address))
                else:
                    deliver(data, address)

            node.sock.sendto.side_effect = send
        return held, deliveries

    def test_multiple_operations_are_in_flight_and_confirm_in_order(self):
        held, deliveries = self.hold_results(sequence=1)
        self.request(1, 10)
        self.wait(lambda: len(held) >= 2)
        self.request(2, 20)
        self.wait(lambda: len(self.leader.pending) == 2)
        self.request(3, 5)
        self.wait(lambda: len(self.leader.pending) == 3 and all(
            op.confirmed(self.leader.view) for op in self.leader.pending.values() if op.sequence > 1))
        self.assertEqual(self.leader.state.num_reqs, 0)
        self.assertEqual(self.bus.replies, [])
        for source, data, address in held:
            deliveries[source](data, address)
        self.wait(lambda: len(self.bus.replies) == 3)
        self.assertEqual([(r.num_reqs, r.total_sum) for r in self.bus.replies],
                         [(1, 10), (2, 30), (3, 35)])
        self.assertTrue(all(node.state.total_sum == 35 for node in self.bus.nodes.values()))

    def test_inflight_retransmission_reuses_record_and_sequence(self):
        held, deliveries = self.hold_results()
        self.request(1, 7)
        self.wait(lambda: len(held) >= 2)
        for _ in range(10):
            self.request(1, 7)
        self.assertEqual(len(self.leader.pending), 1)
        self.assertEqual(len(self.leader.request_workers), 1)
        self.assertEqual(self.leader.next_sequence, 1)
        for source, data, address in held:
            deliveries[source](data, address)
        self.wait(lambda: bool(self.bus.replies))
        self.assertEqual(self.bus.replies[-1].total_sum, 7)
        self.assertEqual(self.leader.state.num_reqs, 1)

    def test_thread_start_failure_does_not_leave_request_or_sequence_reserved(self):
        with patch('server.processing.threading.Thread') as worker:
            worker.return_value.start.side_effect = RuntimeError('cannot start thread')
            self.request(1)
        self.assertEqual(self.leader.pending, {})
        self.assertEqual(self.leader.request_workers, {})
        self.assertEqual(self.leader.next_sequence, 0)
        self.request(1)
        self.wait(lambda: bool(self.bus.replies))
        self.assertEqual(self.bus.replies[-1].total_sum, 7)

    def test_admission_window_bounds_unique_request_workers(self):
        held, deliveries = self.hold_results()
        with patch('server.processing.MAX_INFLIGHT_OPERATIONS', 2):
            self.request(1)
            self.wait(lambda: len(held) >= 2)
            self.request(2)
            self.wait(lambda: len(self.leader.pending) == 2 and sum(
                Message.decode(data).payload['seq'] == 2 for _, data, _ in held) >= 2)
            self.request(3)
            self.assertEqual(len(self.leader.request_workers), 2)
            self.assertEqual(len(self.leader.pending), 2)
            for source, data, address in list(held):
                deliveries[source](data, address)
            self.wait(lambda: len(self.bus.replies) == 2)
        self.request(3)
        self.wait(lambda: len(self.leader.pending) == 1 and sum(
            Message.decode(data).payload['seq'] == 3 for _, data, _ in held) >= 2)
        for source, data, address in list(held):
            deliveries[source](data, address)
        self.wait(lambda: len(self.bus.replies) == 3)
        self.assertEqual(self.leader.state.num_reqs, 3)

    def test_lost_old_result_is_recovered_after_replicas_advance(self):
        held, deliveries = self.hold_results(sequence=1)
        self.request(1, 10)
        self.wait(lambda: len(held) >= 2)
        self.request(2, 20)
        self.wait(lambda: all(n.state.num_reqs == 2 for n in (self.first, self.second)))
        self.assertEqual(self.leader.state.num_reqs, 0)
        self.assertTrue(all(1 in n.result_cache for n in (self.first, self.second)))
        # Drop the original replies permanently; only a cached response to retry can finish.
        for node in (self.first, self.second):
            node.sock.sendto.side_effect = deliveries[node.ip]
        self.wait(lambda: len(self.bus.replies) == 2)
        self.assertEqual([(r.num_reqs, r.total_sum) for r in self.bus.replies], [(1, 10), (2, 30)])
        self.wait(lambda: all(not n.result_cache for n in (self.first, self.second)))

    def test_same_client_requests_wait_for_their_predecessor(self):
        held, deliveries = self.hold_results(sequence=1)
        self.request(1, 10)
        self.wait(lambda: len(held) >= 2)
        self.request(1, 5, request_id=2)
        self.assertEqual(len(self.leader.pending), 1)
        self.request(2, 20)
        self.wait(lambda: len(self.leader.pending) == 2)
        for source, data, address in held:
            deliveries[source](data, address)
        self.wait(lambda: len(self.bus.replies) == 3)
        self.assertEqual(self.leader.state.num_reqs, 3)
        self.assertEqual(self.leader.state.total_sum, 35)
        self.assertFalse(any(r.type == 'HISTORY_GAP' for r in self.bus.replies))

    def test_failed_replica_releases_all_pending_operations_only_after_three_losses(self):
        held = []
        deliver = self.first.sock.sendto.side_effect

        def send(data, address):
            if Message.decode(data).type == 'RESULT':
                held.append(data)
            else:
                deliver(data, address)

        self.first.sock.sendto.side_effect = send
        for client in range(1, 5):
            self.request(client, client)
        self.wait(lambda: len(self.leader.pending) == 4 and self.second.state.num_reqs == 4)
        del self.bus.nodes[self.first.ip]
        now = [100.0]
        with patch('server.heartbeat.CLOCK', side_effect=lambda: now[0]):
            self.leader.tick()
            self.bus.drain()
            monitor = self.leader.monitor_for(self.first.ip)
            for count in (1, 2):
                now[0] = monitor.deadline
                self.leader.tick()
                self.bus.drain()
                self.assertEqual(monitor.failures, count)
                self.assertEqual(self.bus.replies, [])
            now[0] = monitor.deadline
            self.leader.tick()
        self.wait(lambda: len(self.bus.replies) == 4)
        self.assertEqual([(r.num_reqs, r.total_sum) for r in self.bus.replies],
                         [(1, 1), (2, 3), (3, 6), (4, 10)])
        self.assertEqual(self.second.state.total_sum, 10)

    def test_two_rms_crash_together_with_several_operations_pending(self):
        survivor = self.bus.add('127.0.0.4')
        survivor.tick()
        self.bus.drain()
        for node in (self.first, self.second):
            deliver = node.sock.sendto.side_effect

            def send(data, address, deliver=deliver):
                if Message.decode(data).type != 'RESULT':
                    deliver(data, address)

            node.sock.sendto.side_effect = send
        for client in range(1, 5):
            self.request(client, client)
        self.wait(lambda: len(self.leader.pending) == 4 and survivor.state.num_reqs == 4)
        del self.bus.nodes[self.first.ip]
        del self.bus.nodes[self.second.ip]
        now = [100.0]
        with patch('server.heartbeat.CLOCK', side_effect=lambda: now[0]):
            self.leader.tick()
            self.bus.drain()
            for _ in range(3):
                now[0] = max(self.leader.monitor_for(n.ip).deadline for n in (self.first, self.second))
                self.leader.tick()
                self.bus.drain()
        self.wait(lambda: len(self.bus.replies) == 4)
        self.assertEqual(set(self.leader.view), {self.leader.ip, survivor.ip})
        self.assertEqual([r.total_sum for r in self.bus.replies], [1, 3, 6, 10])
        self.assertEqual(survivor.state.snapshot(), self.leader.state.snapshot())

    def test_fe_crash_cancels_pending_workers_and_recovers_applied_prefix(self):
        held, _ = self.hold_results(sequence=1)
        self.request(1, 10)
        self.wait(lambda: len(held) >= 2)
        self.request(2, 20)
        self.request(3, 5)
        self.wait(lambda: len(self.leader.pending) == 3 and all(
            n.state.num_reqs == 3 for n in (self.first, self.second)))
        operations = list(self.leader.pending.values())
        old_epoch = self.leader.epoch
        with self.leader.cv:
            self.leader.ready = False
            self.leader.cancel_work()  # Emulate the worker termination of a crashed process.
        del self.bus.nodes[self.leader.ip]
        self.first.elect()
        self.wait(lambda: self.second.leader == self.second.ip and self.second.ready
                  and self.first.leader == self.second.ip and self.first.ready)
        self.assertTrue(all(operation.cancelled for operation in operations))
        self.assertEqual(self.bus.replies, [])
        self.assertNotEqual(self.second.epoch, old_epoch)
        self.assertEqual(self.second.state.total_sum, 35)
        for client, value, expected in ((1, 10, 10), (2, 20, 30), (3, 5, 35)):
            reply = self.bus.submit(self.second, 1, value, session=f'{client:032x}')
            self.assertEqual(reply.total_sum, expected)
        self.assertEqual(self.second.state.num_reqs, 3)
        self.wait(lambda: not self.leader.request_workers)

    def test_fe_crash_with_missing_first_operation_does_not_apply_old_waiters(self):
        for node in (self.first, self.second):
            receive = node.handle

            def handle(message, address, receive=receive):
                if message.type == 'OP' and message.payload['op'][0] == 1:
                    return
                receive(message, address)

            node.handle = handle
        self.request(1, 10)
        self.wait(lambda: len(self.leader.pending) == 1)
        self.request(2, 20)
        self.request(3, 5)
        self.wait(lambda: all(set(n.buffer) == {2, 3} for n in (self.first, self.second)))
        for node in (self.first, self.second):
            # Restore the dispatcher before the new epoch retransmits sequence 1.
            node.handle = type(node).handle.__get__(node)
        with self.leader.cv:
            self.leader.ready = False
            self.leader.cancel_work()
        del self.bus.nodes[self.leader.ip]
        self.first.elect()
        self.wait(lambda: self.second.leader == self.second.ip and self.second.ready and self.first.ready)
        self.assertEqual(self.second.state.num_reqs, 0)
        for client, value, total in ((1, 10, 10), (2, 20, 30), (3, 5, 35)):
            self.assertEqual(self.bus.submit(self.second, 1, value, session=f'{client:032x}').total_sum, total)
        self.assertEqual(self.second.state.num_reqs, 3)
        self.wait(lambda: not self.first.replica_jobs)

    def test_join_barrier_drains_pipeline_and_includes_new_rm_on_next_operation(self):
        held, deliveries = self.hold_results()
        self.request(1)
        self.request(2)
        self.wait(lambda: len(held) >= 4)
        newcomer = self.bus.add('127.0.0.4')
        newcomer.tick()
        self.bus.drain()
        self.request(3)
        self.wait(lambda: len(self.leader.request_workers) == 3)
        self.assertEqual(len(self.leader.pending), 2)
        self.assertIn(newcomer.ip, self.leader.join_waiters)
        self.assertNotIn(newcomer.ip, self.leader.view)
        for node in (self.first, self.second):
            node.sock.sendto.side_effect = deliveries[node.ip]
        for source, data, address in held:
            deliveries[source](data, address)
        self.wait(lambda: len(self.bus.replies) == 3)
        self.assertIn(newcomer.ip, self.leader.view)
        self.assertEqual(newcomer.state.total_sum, 21)

    def test_dead_joiner_does_not_block_new_requests_forever(self):
        held, deliveries = self.hold_results()
        self.request(1)
        self.wait(lambda: len(held) >= 2)
        self.leader.handle(Message('JOIN', snap=[0, 0, []]), ('127.0.0.9', 4000))
        self.assertIn('127.0.0.9', self.leader.join_waiters)
        for source, data, address in held:
            deliveries[source](data, address)
        for node in (self.first, self.second):
            node.sock.sendto.side_effect = deliveries[node.ip]
        self.wait(lambda: len(self.bus.replies) == 1)
        self.request(2)
        self.assertEqual(len(self.leader.pending), 0)
        now = [100.0]
        with patch('server.heartbeat.CLOCK', side_effect=lambda: now[0]):
            self.leader.tick()
            self.bus.drain()
            monitor = self.leader.monitor_for('127.0.0.9')
            for _ in range(3):
                now[0] = monitor.deadline
                self.leader.tick()
                self.bus.drain()
        self.wait(lambda: len(self.bus.replies) == 2)
        self.assertNotIn('127.0.0.9', self.leader.join_waiters)
        self.assertEqual(self.leader.state.total_sum, 14)

    def test_many_clients_have_unique_sequences_and_identical_prefix_results(self):
        sent = {}
        deliver = self.leader.sock.sendto.side_effect

        def send(data, address):
            message = Message.decode(data)
            if message.type == 'OP':
                operation = message.payload['op']
                previous = sent.setdefault(operation[0], tuple(operation))
                self.assertEqual(previous, tuple(operation))
            deliver(data, address)

        self.leader.sock.sendto.side_effect = send
        for client in range(1, 33):
            self.request(client, client)
        self.wait(lambda: len(self.bus.replies) == 32)
        total = 0
        for sequence, reply in enumerate(self.bus.replies, start=1):
            total += sent[sequence][3]
            self.assertEqual((reply.num_reqs, reply.total_sum), (sequence, total))
        self.assertEqual(total, sum(range(1, 33)))
        self.assertTrue(all(n.state.total_sum == total for n in self.bus.nodes.values()))
