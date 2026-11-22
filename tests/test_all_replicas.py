import time
import unittest
from unittest.mock import patch

from common.protocol import Message
from test_cluster import Bus


class TestAllReplicas(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        for module in ('server.cluster', 'server.heartbeat', 'server.election'):
            timer = patch(module + '.CLOCK', side_effect=lambda: self.now)
            timer.start()
            self.addCleanup(timer.stop)
        self.bus = Bus()
        self.a, self.b, self.c = self.bus.cluster()
        self.d = self.bus.add('127.0.0.4')
        self.d.tick()
        self.bus.drain()

    def pump_until(self, predicate):
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            self.bus.drain()
            if predicate():
                return
            time.sleep(0.001)
        self.fail('Expected protocol event did not complete')

    def hold_results(self, *nodes):
        held = {node.ip: [] for node in nodes}
        deliveries = {}
        for node in nodes:
            deliver = node.sock.sendto.side_effect
            deliveries[node.ip] = deliver

            def send(data, address, node=node, deliver=deliver):
                if Message.decode(data).type == 'RESULT':
                    held[node.ip].append((data, address))
                else:
                    deliver(data, address)

            node.sock.sendto.side_effect = send
        return held, deliveries

    def start_request(self, request_id=1, value=7):
        with self.a.cv:
            self.a.handle(Message.request(request_id, value, session='a' * 32), ('client', 4000))

    def finish_even_on_failure(self):
        # Release any waiting worker so a failed assertion does not leak its thread.
        for node in list(self.bus.nodes.values()):
            with node.cv:
                node.ready = False
                node.cancel_work()

    def allow_worker_to_run(self):
        deadline = time.monotonic() + 0.03
        while time.monotonic() < deadline:
            self.bus.drain()
            time.sleep(0.001)

    def test_one_of_three_blocks_and_two_of_three_confirms_majority(self):
        held, deliveries = self.hold_results(self.c, self.d)
        self.start_request()
        try:
            self.pump_until(lambda: bool(held[self.c.ip]) and bool(held[self.d.ip]))
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertEqual(self.a.state.num_reqs, 0)
            # Release only one of the two held RMs -> 2 of 3 confirms majority immediately.
            for data, address in held[self.c.ip]:
                deliveries[self.c.ip](data, address)
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 7)
            self.assertEqual(self.a.state.total_sum, 7)
            self.assertEqual(self.d.state.total_sum, 7)
        finally:
            self.finish_even_on_failure()

    def test_confirmed_operation_survives_and_blocks_until_new_replica_joins(self):
        self.assertEqual(self.bus.submit(self.a, 1).total_sum, 7)
        self.assertTrue(all(node.state.total_sum == 7 for node in (self.b, self.c, self.d)))
        for node in (self.a, self.b, self.c):
            del self.bus.nodes[node.ip]
        self.d.elect()
        for _ in range(20):
            self.bus.drain()
            if self.d.leader == self.d.ip and self.d.ready:
                break
            self.d.tick()
            self.now += self.d.next_timeout() + 0.000001
        self.assertTrue(self.d.ready)
        self.assertEqual(self.d.leader, self.d.ip)
        self.assertEqual(self.d.state.total_sum, 7)
        self.bus.replies.clear()
        with self.d.cv:
            self.d.handle(Message.request(2, 5, session='a' * 32), ('client', 4000))
        try:
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertEqual(self.d.state.num_reqs, 1)
            newcomer = self.bus.add('127.0.0.5')
            newcomer.tick()
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 12)
            self.assertEqual(newcomer.state.total_sum, 12)
        finally:
            self.finish_even_on_failure()

    def test_result_loss_with_healthy_heartbeats_does_not_remove_replica(self):
        held, deliveries = self.hold_results(self.c, self.d)
        self.start_request()
        try:
            self.pump_until(lambda: bool(held[self.c.ip]) and bool(held[self.d.ip]))
            for _ in range(10):
                for node in self.bus.nodes.values():
                    node.tick()
                self.bus.drain()
                self.now += 0.05
            self.assertIn(self.c.ip, self.a.view)
            self.assertIn(self.d.ip, self.a.view)
            self.assertEqual(self.a.monitor_for(self.d.ip).failures, 0)
            self.assertEqual(self.bus.replies, [])
            for data, address in held[self.c.ip]:
                deliveries[self.c.ip](data, address)
            self.pump_until(lambda: bool(self.bus.replies))
        finally:
            self.finish_even_on_failure()

    def test_operation_lost_by_live_replicas_is_retried_before_client_ack(self):
        receivers = {node.ip: node.handle for node in (self.c, self.d)}
        dropped = {self.c.ip: [], self.d.ip: []}

        for node in (self.c, self.d):
            receive = receivers[node.ip]

            def handle(message, address, node=node, receive=receive):
                if message.type == 'OP':
                    dropped[node.ip].append(message)
                else:
                    receive(message, address)

            node.handle = handle
        self.start_request()
        try:
            self.pump_until(lambda: bool(dropped[self.c.ip]) and bool(dropped[self.d.ip]))
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertEqual(self.c.state.total_sum, 0)
            self.assertEqual(self.d.state.total_sum, 0)
            self.assertEqual(self.a.state.total_sum, 0)
            for node in (self.c, self.d):
                node.handle = receivers[node.ip]
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 7)
            self.pump_until(lambda: all(node.state.total_sum == 7 for node in self.bus.nodes.values()))
        finally:
            for node in (self.c, self.d):
                node.handle = receivers[node.ip]
            self.finish_even_on_failure()

    def test_new_replica_joins_after_pending_request_and_counts_in_next_quorum(self):
        held, deliveries = self.hold_results(self.c, self.d)
        self.start_request()
        try:
            self.pump_until(lambda: bool(held[self.c.ip]) and bool(held[self.d.ip]))
            newcomer = self.bus.add('127.0.0.5')
            newcomer.tick()
            self.bus.drain()
            self.assertNotIn(newcomer.ip, self.a.view)
            self.assertNotIn(newcomer.ip, next(iter(self.a.pending.values())).members)
            for node in (self.c, self.d):
                node.sock.sendto.side_effect = deliveries[node.ip]
                for data, address in held[node.ip]:
                    deliveries[node.ip](data, address)
            self.pump_until(lambda: bool(self.bus.replies))
            self.a.announce()
            self.bus.drain()
            self.assertIn(newcomer.ip, self.a.view)
            self.assertEqual(newcomer.state.total_sum, 7)
            self.bus.replies.clear()
            # With 4 RMs (b, c, d, newcomer), majority is 3: holding d and newcomer blocks at 2/4.
            next_held, next_deliveries = self.hold_results(self.d, newcomer)
            self.start_request(request_id=2, value=7)
            self.pump_until(lambda: bool(next_held[self.d.ip]) and bool(next_held[newcomer.ip]))
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertIn(newcomer.ip, next(iter(self.a.pending.values())).members)
            for data, address in next_held[newcomer.ip]:
                next_deliveries[newcomer.ip](data, address)
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 14)
        finally:
            self.finish_even_on_failure()

    def test_only_third_heartbeat_loss_releases_wait_for_crashed_replicas(self):
        held, _ = self.hold_results(self.c, self.d)
        self.start_request()
        try:
            self.pump_until(lambda: bool(held[self.c.ip]) and bool(held[self.d.ip]))
            del self.bus.nodes[self.c.ip]
            del self.bus.nodes[self.d.ip]
            self.a.tick()
            self.bus.drain()
            monitors = [self.a.monitor_for(self.c.ip), self.a.monitor_for(self.d.ip)]
            for count in (1, 2):
                self.now = max(m.deadline for m in monitors)
                self.a.tick()
                self.bus.drain()
                for monitor in monitors:
                    self.assertEqual(monitor.failures, count)
                self.assertIn(self.d.ip, self.a.view)
                self.assertEqual(self.bus.replies, [])
            self.now = max(m.deadline for m in monitors)
            self.a.tick()
            self.bus.drain()
            self.assertEqual(set(self.a.view), {self.a.ip, self.b.ip})
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 7)
            self.assertEqual(self.a.state.num_reqs, 1)
        finally:
            self.finish_even_on_failure()

    def test_foreign_duplicate_or_wrong_result_cannot_replace_missing_replica(self):
        held, deliveries = self.hold_results(self.c, self.d)
        self.start_request()
        try:
            self.pump_until(lambda: bool(held[self.c.ip]) and bool(held[self.d.ip]))
            with self.a.cv:
                for source, epoch, total in (
                    ('127.0.0.99', self.a.epoch, 7),
                    (self.b.ip, self.a.epoch, 7),
                    (self.d.ip, 'previous-epoch', 7),
                    (self.d.ip, self.a.epoch, 99),
                ):
                    self.a.handle(Message('RESULT', epoch=epoch, seq=1, total=total), (source, 4000))
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertTrue(all('127.0.0.99' not in operation.acks for operation in self.a.pending.values()))
            for data, address in held[self.c.ip]:
                deliveries[self.c.ip](data, address)
            self.pump_until(lambda: bool(self.bus.replies))
        finally:
            self.finish_even_on_failure()
