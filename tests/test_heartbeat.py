import unittest
import threading
from unittest.mock import Mock, patch

from common.protocol import Message
from server import election, transport
from server.timing import PeerMonitor
from test_cluster import Bus


class TestPeerMonitor(unittest.TestCase):
    def test_initial_timeout_and_three_times_moving_average(self):
        monitor = PeerMonitor()
        self.assertEqual(monitor.timeout, 0.1)
        monitor.observe(0.1)
        self.assertAlmostEqual(monitor.timeout, 0.3)
        monitor.observe(0.3)
        self.assertAlmostEqual(monitor.average_rtt, 0.125)
        self.assertAlmostEqual(monitor.timeout, 0.375)

    def test_floor_is_configurable_and_slow_network_is_not_capped(self):
        monitor = PeerMonitor(minimum=0.025)
        monitor.observe(0.001)
        self.assertEqual(monitor.timeout, 0.025)
        monitor = PeerMonitor()
        monitor.observe(2.0)
        self.assertEqual(monitor.timeout, 6.0)

    def test_only_matching_timely_reply_resets_losses(self):
        monitor = PeerMonitor()
        monitor.failures = 2
        probe = monitor.start(100)
        self.assertFalse(monitor.acknowledge('old', 100.001))
        self.assertEqual(monitor.failures, 2)
        self.assertTrue(monitor.acknowledge(probe, 100.001))
        self.assertEqual(monitor.failures, 0)
        self.assertAlmostEqual(monitor.average_rtt, 0.001)
        self.assertFalse(monitor.acknowledge(probe, 100.002))

    def test_late_reply_is_not_a_valid_sample(self):
        monitor = PeerMonitor()
        monitor.failures = 2
        probe = monitor.start(100)
        self.assertFalse(monitor.acknowledge(probe, 100.101))
        self.assertEqual(monitor.failures, 2)
        self.assertIsNone(monitor.average_rtt)

    def test_invalid_samples_are_ignored(self):
        monitor = PeerMonitor()
        for sample in (-1, float('inf'), float('nan')):
            monitor.observe(sample)
        self.assertIsNone(monitor.average_rtt)


class TestHeartbeat(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        for module in ('server.cluster', 'server.heartbeat', 'server.election'):
            clock = patch(module + '.CLOCK', side_effect=lambda: self.now)
            clock.start()
            self.addCleanup(clock.stop)
        self.bus = Bus()
        self.leader, self.first, self.second = self.bus.cluster()

    def warm(self, node):
        node.tick()
        self.bus.drain()

    def test_two_losses_keep_leader_and_third_starts_election(self):
        follower = self.first
        self.warm(follower)
        monitor = follower.monitor_for(self.leader.ip)
        self.assertIsNotNone(monitor.average_rtt)
        del self.bus.nodes[self.leader.ip]
        self.now = monitor.next_probe
        follower.tick()
        for count in (1, 2):
            previous_probe = monitor.probe
            self.now = monitor.deadline
            follower.tick()
            self.assertEqual(monitor.failures, count)
            self.assertEqual(follower.leader, self.leader.ip)
            self.assertNotEqual(monitor.probe, previous_probe)
        self.now = monitor.deadline
        follower.tick()
        self.assertIsNone(follower.leader)
        self.assertIsNotNone(follower.ballot)

    def test_valid_response_breaks_sequence_of_losses(self):
        follower = self.first
        follower.tick()
        monitor = follower.monitor_for(self.leader.ip)
        self.now = monitor.deadline
        follower.tick()
        self.assertEqual(monitor.failures, 1)
        self.bus.drain()  # Old probe is ignored; the current one resets the losses.
        self.assertEqual(monitor.failures, 0)
        self.assertIsNone(monitor.probe)

    def test_long_scheduler_pause_counts_one_probe_not_three(self):
        self.first.tick()
        monitor = self.first.monitor_for(self.leader.ip)
        self.now += 10
        self.first.tick()
        self.assertEqual(monitor.failures, 1)
        self.assertEqual(self.first.leader, self.leader.ip)

    def test_only_third_loss_removes_replica(self):
        self.leader.tick()
        self.bus.drain()
        del self.bus.nodes[self.first.ip]
        monitor = self.leader.monitor_for(self.first.ip)
        self.now = monitor.next_probe
        self.leader.tick()
        for _ in range(2):
            self.now = monitor.deadline
            self.leader.tick()
            self.bus.drain()
            self.assertIn(self.first.ip, self.leader.view)
        self.now = monitor.deadline
        self.leader.tick()
        self.assertNotIn(self.first.ip, self.leader.view)

    def test_next_wait_follows_nearest_probe_deadline(self):
        self.first.tick()
        monitor = self.first.monitor_for(self.leader.ip)
        self.now = monitor.deadline - 0.007
        self.assertAlmostEqual(self.first.next_timeout(), 0.007)
        self.now = monitor.deadline + 0.001
        self.assertEqual(self.first.next_timeout(), 0)

    def test_wrong_epoch_cannot_reset_failures(self):
        self.first.tick()
        monitor = self.first.monitor_for(self.leader.ip)
        monitor.failures = 2
        message = Message('ALIVE', epoch='old', probe=monitor.probe)
        self.first.handle(message, (self.leader.ip, 4000))
        self.assertEqual(monitor.failures, 2)

    def test_monitoring_recovers_after_leader_crash(self):
        for node in self.bus.nodes.values():
            self.warm(node)
        self.assertEqual(self.bus.submit(self.leader, 1).total_sum, 7)
        del self.bus.nodes[self.leader.ip]
        for _ in range(100):
            for node in list(self.bus.nodes.values()):
                node.tick()
            self.bus.drain()
            if all(node.leader == self.second.ip for node in self.bus.nodes.values()):
                break
            self.now += min(node.next_timeout() for node in self.bus.nodes.values()) + 0.000001
        self.assertEqual(self.first.leader, self.second.ip)
        self.assertEqual(self.second.leader, self.second.ip)
        self.assertEqual(self.bus.submit(self.second, 2).total_sum, 14)

    def test_election_ack_uses_peer_rtt_and_rejects_skipped_peer(self):
        node = self.first
        node.monitors[self.leader.ip] = PeerMonitor(minimum=0.01)
        node.monitors[self.second.ip] = PeerMonitor(minimum=0.01)
        node.monitor_for(self.leader.ip).observe(0.002)
        node.monitor_for(self.second.ip).observe(0.02)
        token = dict(uid='token', path=[node.ip])
        node.hop['token'] = [token, [self.leader.ip, self.second.ip], 0, 0, 0]
        node.retry('token')
        self.assertAlmostEqual(node.hop['token'][4], self.now + 0.01)
        self.now += 0.011
        node.retry('token')
        self.assertAlmostEqual(node.hop['token'][4], self.now + 0.06)
        ack = Message('HOP_ACK', uid='token', epoch=node.epoch)
        election.handle_hop_ack(node, ack, (self.leader.ip, 4000))
        self.assertIn('token', node.hop)
        self.now += 0.003
        election.handle_hop_ack(node, ack, (self.second.ip, 4000))
        self.assertNotIn('token', node.hop)

    def test_windows_floor_handles_scheduler_delays_without_removing_live_nodes(self):
        delayed = []
        for node in self.bus.nodes.values():
            peers = set(node.view) - {node.ip} if node.leader == node.ip else {node.leader}
            for peer in peers:
                node.monitors[peer] = PeerMonitor(minimum=0.05)
                node.monitors[peer].observe(0.001)
            deliver = node.sock.sendto.side_effect

            def send(data, address, deliver=deliver):
                if Message.decode(data).type == 'ALIVE':
                    delayed.append((self.now + 0.02, deliver, data, address))
                else:
                    deliver(data, address)

            node.sock.sendto.side_effect = send
        for _ in range(100):
            self.now += 0.005
            for item in list(delayed):
                due, deliver, data, address = item
                if self.now >= due:
                    delayed.remove(item)
                    deliver(data, address)
            self.bus.drain()
            for node in self.bus.nodes.values():
                node.tick()
            self.bus.drain()
        self.assertEqual(set(self.leader.view), set(self.bus.nodes))
        for node in self.bus.nodes.values():
            self.assertEqual(node.leader, self.leader.ip)
            self.assertTrue(all(monitor.failures == 0 for monitor in node.monitors.values()))


class TestReceiveDeadlines(unittest.TestCase):
    def test_receive_loop_uses_calculated_wait_instead_of_fixed_polling(self):
        node = Mock()
        node.cv = threading.Condition()
        node.next_timeout.return_value = 0.007
        with patch('server.transport.select.select', side_effect=KeyboardInterrupt) as select:
            with self.assertRaises(KeyboardInterrupt):
                transport.run(node)
        self.assertEqual(select.call_args.args[3], 0.007)
        node.tick.assert_called_once()

    def test_udp_port_unreachable_does_not_crash_receive_loop(self):
        node = Mock()
        node.cv = threading.Condition()
        node.next_timeout.return_value = 0.01
        node.sock.recvfrom.side_effect = ConnectionResetError()
        with patch('server.transport.select.select', side_effect=[
            ([node.sock], [], []), KeyboardInterrupt,
        ]):
            with self.assertRaises(KeyboardInterrupt):
                transport.run(node)
        node.handle.assert_not_called()
        self.assertEqual(node.tick.call_count, 3)
