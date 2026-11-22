"""Integration tests using production dispatch, codecs and handlers on an in-memory UDP bus."""
import collections
import time
import unittest
from unittest.mock import patch

from common.protocol import Message
from server.cluster import Node, GROUP, CLOCK


class Bus:
    def __init__(self):
        self.nodes = {}
        self.messages = collections.deque()
        self.replies = []

    def add(self, ip):
        with patch('server.transport.socket.socket') as factory:
            factory.return_value.getsockname.return_value = (ip, 4000)
            node = Node(4000, True)
        self.nodes[ip] = node
        node.interface.log = lambda *parts: None

        def sendto(data, address):
            destination = address[0]
            peers = list(self.nodes) if destination == GROUP else [destination]
            for peer in peers:
                self.messages.append((peer, ip, data))

        node.sock.sendto.side_effect = sendto
        return node

    def drain(self):
        while self.messages:
            destination, source, data = self.messages.popleft()
            message = Message.decode(data)
            if destination == 'client':
                self.replies.append(message)
            elif destination in self.nodes:
                node = self.nodes[destination]
                with node.cv:
                    node.handle(message, (source, 4000))

    def wait_for(self, predicate, timeout=1):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.drain()
            if predicate():
                return
            time.sleep(0.001)
        raise AssertionError('Expected asynchronous protocol event did not complete')

    def submit(self, node, request_id, value=7, session='a' * 32):
        before = len(self.replies)
        with node.cv:
            node.handle(Message.request(request_id, value, session=session), ('client', 4000))
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            self.drain()
            if len(self.replies) > before:
                return self.replies[-1]
            time.sleep(0.005)
        raise AssertionError('Request did not receive an ACK')

    def cluster(self):
        leader = self.add('127.0.0.1')
        leader.started -= 3
        leader.tick()
        self.drain()
        first = self.add('127.0.0.2')
        second = self.add('127.0.0.3')
        first.tick()
        second.tick()
        self.drain()
        return leader, first, second


class TestCluster(unittest.TestCase):
    def test_replication_failover_duplicate_and_recovery(self):
        bus = Bus()
        leader, first, second = bus.cluster()
        self.assertEqual(set(first.view), set(bus.nodes))
        self.assertEqual(bus.submit(leader, 1).total_sum, 7)
        self.assertEqual(first.state.total_sum, 7)
        self.assertEqual(second.state.total_sum, 7)
        del bus.nodes[leader.ip]
        first.elect()
        second.elect()
        bus.drain()
        self.assertEqual(first.leader, second.ip)
        self.assertEqual(bus.submit(second, 1).total_sum, 7)
        self.assertEqual(bus.submit(second, 2).total_sum, 14)
        del bus.nodes[first.ip]
        # Three distinct unanswered heartbeats, rather than elapsed wall time alone.
        for _ in range(3):
            monitor = second.monitor_for(first.ip)
            monitor.start(CLOCK() - 1)
            second.tick()
        bus.drain()
        self.assertEqual(second.view, [second.ip])
        first = bus.add(first.ip)
        first.tick()
        bus.drain()
        self.assertEqual(first.state.snapshot(), second.state.snapshot())
        self.assertEqual(bus.submit(second, 3).total_sum, 21)
        self.assertEqual(first.state.snapshot(), second.state.snapshot())

    def test_multiple_clients_and_gap_use_original_state_rules(self):
        bus = Bus()
        leader, first, second = bus.cluster()
        self.assertEqual(bus.submit(leader, 1, 10).total_sum, 10)
        gap = bus.submit(leader, 3, 99)
        self.assertEqual(gap.type, 'HISTORY_GAP')
        self.assertEqual((gap.id_req, gap.payload['last_req'], gap.num_reqs, gap.total_sum), (3, 1, 1, 10))
        self.assertEqual(bus.submit(leader, 1, 5, session='b' * 32).total_sum, 15)
        self.assertEqual(bus.submit(leader, 2, 3).total_sum, 18)
        for node in (leader, first, second):
            self.assertEqual((node.state.num_reqs, node.state.total_sum), (3, 18))

    def test_only_leader_answers_discovery(self):
        bus = Bus()
        leader, first, second = bus.cluster()
        for node in (leader, first, second):
            with node.cv:
                node.handle(Message.discovery(), ('client', 4000))
        bus.drain()
        self.assertEqual(len(bus.replies), 1)
        self.assertEqual(bus.replies[0].type, 'DISCOVERY_RESP')
        self.assertIsNotNone(leader.state.get_client('client', 4000))

    def test_ack_waits_for_replica_quorum(self):
        bus = Bus()
        leader, first, second = bus.cluster()
        held = []
        deliver = second.sock.sendto.side_effect

        def hold_result(data, address):
            if Message.decode(data).type == 'RESULT':
                held.append((data, address))
            else:
                deliver(data, address)

        second.sock.sendto.side_effect = hold_result
        leader.handle(Message.request(1, 7, session='a' * 32), ('client', 4000))
        try:
            deadline = time.monotonic() + 1
            while not held and time.monotonic() < deadline:
                bus.drain()
                time.sleep(0.005)
            self.assertTrue(held)
            self.assertEqual(first.state.total_sum, 7)
            self.assertEqual(second.state.total_sum, 7)
            self.assertEqual(leader.state.total_sum, 0)
            self.assertEqual(bus.replies, [])
            for data, address in held:
                deliver(data, address)
            deadline = time.monotonic() + 1
            while not bus.replies and time.monotonic() < deadline:
                bus.drain()
                time.sleep(0.005)
            self.assertEqual(bus.replies[-1].total_sum, 7)
        finally:
            with leader.cv:
                leader.ready = False
                leader.cancel_work()

    def test_follower_ignores_client_requests(self):
        bus = Bus()
        leader, first, second = bus.cluster()
        first.handle(Message.request(1, 5, session='a' * 32), ('client', 4000))
        bus.drain()
        self.assertEqual(bus.replies, [])
        self.assertEqual(first.state.total_sum, 0)

    def test_replicas_reorder_operations_before_applying(self):
        bus = Bus()
        leader, first, second = bus.cluster()
        for sequence, value in ((2, 4), (1, 3)):
            message = Message('OP', epoch=leader.epoch,
                              op=[sequence, 'a' * 32, sequence, value, 'client', 4000])
            with first.cv:
                first.handle(message, (leader.ip, 4000))
        bus.wait_for(lambda: first.state.num_reqs == 2)
        self.assertEqual((first.state.num_reqs, first.state.total_sum), (2, 7))
        self.assertEqual(first.buffer, {})

    def test_exit_keeps_duplicate_history(self):
        bus = Bus()
        leader, _, _ = bus.cluster()
        bus.submit(leader, 1)
        leader.handle(Message.exit(), ('client', 4000))
        self.assertEqual(bus.submit(leader, 1).total_sum, 7)


if __name__ == '__main__':
    unittest.main()
