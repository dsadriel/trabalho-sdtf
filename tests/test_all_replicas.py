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

    def hold_last_result(self):
        held = []
        deliver = self.d.sock.sendto.side_effect

        def send(data, address):
            if Message.decode(data).type == 'RESULT':
                held.append((data, address))
            else:
                deliver(data, address)

        self.d.sock.sendto.side_effect = send
        return held, deliver

    def start_request(self):
        with self.a.cv:
            self.a.handle(Message.request(1, 7, session='a' * 32), ('client', 4000))

    def finish_even_on_failure(self):
        # Release a waiting worker so a failed assertion does not leak its thread.
        with self.a.cv:
            self.a.view = [self.a.ip]
            self.a.cv.notify_all()

    def allow_worker_to_run(self):
        deadline = time.monotonic() + 0.03
        while time.monotonic() < deadline:
            self.bus.drain()
            time.sleep(0.001)

    def test_two_of_three_results_do_not_allow_client_ack(self):
        held, deliver = self.hold_last_result()
        self.start_request()
        try:
            self.pump_until(lambda: bool(held))
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertEqual(self.a.state.num_reqs, 0)
            for node in (self.b, self.c, self.d):
                self.assertEqual(node.state.total_sum, 7)
            for data, address in held:
                deliver(data, address)
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 7)
        finally:
            self.finish_even_on_failure()

    def test_confirmed_operation_survives_when_only_last_replica_remains(self):
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
        self.assertEqual(self.bus.submit(self.d, 2, 5).total_sum, 12)

    def test_result_loss_with_healthy_heartbeats_does_not_remove_replica(self):
        held, deliver = self.hold_last_result()
        self.start_request()
        try:
            self.pump_until(lambda: bool(held))
            for _ in range(10):
                for node in self.bus.nodes.values():
                    node.tick()
                self.bus.drain()
                self.now += 0.05
            self.assertIn(self.d.ip, self.a.view)
            self.assertEqual(self.a.monitor_for(self.d.ip).failures, 0)
            self.assertEqual(self.bus.replies, [])
            for data, address in held:
                deliver(data, address)
            self.pump_until(lambda: bool(self.bus.replies))
        finally:
            self.finish_even_on_failure()

    def test_operation_lost_by_live_replica_is_retried_before_client_ack(self):
        receive = self.d.handle
        dropped = []

        def handle(message, address):
            if message.type == 'OP':
                dropped.append(message)
            else:
                receive(message, address)

        self.d.handle = handle
        self.start_request()
        try:
            self.pump_until(lambda: bool(dropped))
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertEqual(self.d.state.total_sum, 0)
            self.assertEqual(self.a.state.total_sum, 0)
            self.d.handle = receive
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 7)
            self.assertTrue(all(node.state.total_sum == 7 for node in self.bus.nodes.values()))
        finally:
            self.d.handle = receive
            self.finish_even_on_failure()

    def test_new_replica_joins_after_pending_request_and_is_required_on_next(self):
        held, deliver = self.hold_last_result()
        self.start_request()
        try:
            self.pump_until(lambda: bool(held))
            newcomer = self.bus.add('127.0.0.5')
            newcomer.tick()
            self.bus.drain()
            self.assertNotIn(newcomer.ip, self.a.view)
            self.assertNotIn(newcomer.ip, next(iter(self.a.pending.values())).members)
            for data, address in held:
                deliver(data, address)
            self.pump_until(lambda: bool(self.bus.replies))
            self.a.announce()
            self.bus.drain()
            self.assertIn(newcomer.ip, self.a.view)
            self.assertEqual(newcomer.state.total_sum, 7)
            self.bus.replies.clear()
            next_results = []
            send = newcomer.sock.sendto.side_effect

            def hold(data, address):
                if Message.decode(data).type == 'RESULT':
                    next_results.append((data, address))
                else:
                    send(data, address)

            newcomer.sock.sendto.side_effect = hold
            # The older held sender must now deliver the next operation normally.
            self.d.sock.sendto.side_effect = deliver
            self.a.handle(Message.request(2, 7, session='a' * 32), ('client', 4000))
            self.pump_until(lambda: bool(next_results))
            self.allow_worker_to_run()
            self.assertEqual(self.bus.replies, [])
            self.assertIn(newcomer.ip, next(iter(self.a.pending.values())).members)
            for data, address in next_results:
                send(data, address)
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 14)
        finally:
            self.finish_even_on_failure()

    def test_only_third_heartbeat_loss_releases_wait_for_crashed_replica(self):
        held, _ = self.hold_last_result()
        self.start_request()
        try:
            self.pump_until(lambda: bool(held))
            del self.bus.nodes[self.d.ip]
            self.a.tick()
            self.bus.drain()
            monitor = self.a.monitor_for(self.d.ip)
            for count in (1, 2):
                self.now = monitor.deadline
                self.a.tick()
                self.bus.drain()
                self.assertEqual(monitor.failures, count)
                self.assertIn(self.d.ip, self.a.view)
                self.assertEqual(self.bus.replies, [])
            self.now = monitor.deadline
            self.a.tick()
            self.bus.drain()
            self.assertNotIn(self.d.ip, self.a.view)
            self.pump_until(lambda: bool(self.bus.replies))
            self.assertEqual(self.bus.replies[-1].total_sum, 7)
            self.assertEqual(self.a.state.num_reqs, 1)
        finally:
            self.finish_even_on_failure()

    def test_foreign_duplicate_or_wrong_result_cannot_replace_missing_replica(self):
        held, deliver = self.hold_last_result()
        self.start_request()
        try:
            self.pump_until(lambda: bool(held))
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
            for data, address in held:
                deliver(data, address)
            self.pump_until(lambda: bool(self.bus.replies))
        finally:
            self.finish_even_on_failure()
