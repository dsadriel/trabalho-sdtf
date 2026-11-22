import time
import unittest
from unittest.mock import patch

from common.protocol import Message
from test_cluster import Bus


class TestFailover(unittest.TestCase):
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
        self.session = 'a' * 32

    def seed(self, node, request, value):
        return node.state.process_request('client', request, value, 4000, session=self.session)

    def pump(self, predicate, rounds=100):
        for _ in range(rounds):
            self.bus.drain()
            if predicate():
                return
            for node in list(self.bus.nodes.values()):
                node.tick()
            self.bus.drain()
            if predicate():
                return
            waits = [n.next_timeout() for n in self.bus.nodes.values()]
            self.now += min(w for w in waits if w is not None) + 0.000001
        self.fail('Cluster did not converge')

    def assert_followers(self):
        for node in (self.b, self.c, self.d):
            self.assertEqual(node.leader, self.d.ip)
            self.assertTrue(node.ready)
            self.assertEqual(node.state.snapshot(), self.d.state.snapshot())

    def test_single_detector_election_updates_every_participant(self):
        self.bus.submit(self.a, 1)
        del self.bus.nodes[self.a.ip]
        self.c.elect()  # b and d have not detected the old leader's failure yet.
        self.bus.drain()
        self.assert_followers()

    def test_candidate_recovers_operation_present_on_other_participants(self):
        self.bus.submit(self.a, 1)
        for node in (self.a, self.b, self.c):
            self.seed(node, 2, 11)
        self.assertEqual(self.d.state.num_reqs, 1)
        del self.bus.nodes[self.a.ip]
        self.c.elect()
        self.bus.drain()
        self.assert_followers()
        self.assertEqual(self.d.state.total_sum, 18)
        self.assertEqual(self.bus.submit(self.d, 2, 11).total_sum, 18)
        self.assertEqual(self.bus.submit(self.d, 3, 5).total_sum, 23)

    def test_promotion_does_not_rollback_newer_local_snapshot(self):
        self.bus.submit(self.a, 1)
        self.seed(self.d, 2, 11)
        self.d.view = [self.b.ip, self.c.ip, self.d.ip]
        self.d.promote(self.b.state.snapshot())
        self.bus.drain()
        self.assert_followers()
        self.assertEqual(self.d.state.num_reqs, 2)

    def test_sync_barrier_collects_newer_report_before_serving(self):
        self.bus.submit(self.a, 1)
        self.seed(self.b, 2, 11)
        self.d.view = [self.b.ip, self.c.ip, self.d.ip]
        self.d.promote(self.d.state.snapshot())
        self.assertFalse(self.d.ready)
        self.d.handle(Message.discovery(), ('client', 4000))
        self.assertEqual(self.bus.replies[-1].type, 'ACK')  # Only the earlier ACK exists.
        self.bus.drain()
        self.assert_followers()
        self.assertEqual(self.d.state.total_sum, 18)

    def test_lost_first_announcement_is_retried_before_serving(self):
        self.d.view = [self.b.ip, self.c.ip, self.d.ip]
        original = self.d.sock.sendto.side_effect
        dropped = [0]

        def send(data, address):
            if Message.decode(data).type == 'VIEW' and dropped[0] < 3:
                dropped[0] += 1
                return
            original(data, address)

        self.d.sock.sendto.side_effect = send
        self.d.promote(self.d.state.snapshot())
        self.bus.drain()
        self.assertFalse(self.d.ready)
        self.pump(lambda: self.d.ready)
        self.assert_followers()

    def test_lost_snapshot_is_retried_until_ready_confirmation(self):
        self.d.view = [self.b.ip, self.c.ip, self.d.ip]
        original = self.d.sock.sendto.side_effect
        dropped = [False]

        def send(data, address):
            if Message.decode(data).type == 'SNAP' and address[0] == self.b.ip and not dropped[0]:
                dropped[0] = True
                return
            original(data, address)

        self.d.sock.sendto.side_effect = send
        self.d.promote(self.d.state.snapshot())
        self.bus.drain()
        self.pump(lambda: self.d.ready)
        self.assertTrue(dropped[0])
        self.assert_followers()

    def test_delayed_old_snapshot_does_not_cancel_ongoing_election(self):
        old_epoch = self.a.epoch
        self.c.elect()
        self.c.handle(Message('SNAP', epoch=old_epoch, term=self.a.term,
                              members=self.a.view, snap=self.a.state.snapshot()), (self.a.ip, 4000))
        self.assertIsNone(self.c.leader)
        self.assertIsNotNone(self.c.ballot)

    def test_concurrent_coordinators_of_same_generation_converge(self):
        del self.bus.nodes[self.a.ip]
        members = [self.b.ip, self.c.ip, self.d.ip]
        self.c.view = members.copy()
        self.d.view = members.copy()
        self.c.promote(self.c.state.snapshot(), term=2)
        self.d.promote(self.d.state.snapshot(), term=2)
        self.bus.drain()
        self.assert_followers()

    def test_old_snapshot_and_announcement_cannot_undo_new_leadership(self):
        old_epoch = self.a.epoch
        old_snapshot = self.a.state.snapshot()
        del self.bus.nodes[self.a.ip]
        self.c.elect()
        self.bus.drain()
        for kind, fields in (
            ('VIEW', dict(epoch=old_epoch, term=1, members=[self.a.ip, self.b.ip])),
            ('SNAP', dict(epoch=old_epoch, term=1, snap=old_snapshot)),
        ):
            self.b.handle(Message(kind, **fields), (self.a.ip, 4000))
        self.assertEqual(self.b.leader, self.d.ip)
        self.assertEqual(self.b.epoch, self.d.epoch)

    def test_quorum_wait_cancels_when_all_replicas_crash_and_resumes_after_rejoin(self):
        self.bus.submit(self.a, 1)
        del self.bus.nodes[self.a.ip]
        self.c.elect()
        self.bus.drain()
        self.assert_followers()
        self.assertEqual(self.bus.submit(self.d, 2).total_sum, 14)
        del self.bus.nodes[self.b.ip]
        del self.bus.nodes[self.c.ip]
        self.bus.replies.clear()
        self.d.handle(Message.request(3, 5, session=self.session), ('client', 4000))
        deadline = time.monotonic() + 1
        while not self.d.pending and time.monotonic() < deadline:
            self.bus.drain()
            time.sleep(0.001)
        self.assertTrue(self.d.pending)
        self.d.tick()
        self.bus.drain()
        for _ in range(3):
            self.now = max(self.d.monitor_for(p).deadline for p in (self.b.ip, self.c.ip))
            self.d.tick()
            self.bus.drain()
        self.assertEqual(self.d.view, [self.d.ip])
        self.assertEqual(self.d.pending, {})
        self.assertEqual(self.bus.replies, [])
        self.assertEqual(self.d.state.total_sum, 14)
        # Rejoining one RM restores quorum and allows client retry to succeed.
        rejoined = self.bus.add(self.b.ip)
        rejoined.tick()
        self.bus.drain()
        self.assertEqual(self.bus.submit(self.d, 3, 5).total_sum, 19)
        self.assertEqual(self.bus.submit(self.d, 4, 3).total_sum, 22)
        self.assertEqual(rejoined.state.total_sum, 22)

    def test_missing_history_is_reported_without_modifying_sum(self):
        self.d.view = [self.d.ip]
        self.d.promote(self.d.state.snapshot())
        self.bus.drain()
        self.seed(self.d, 1, 7)
        reply = self.bus.submit(self.d, 3, 99)
        self.assertEqual(reply.type, 'HISTORY_GAP')
        self.assertEqual(reply.payload['last_req'], 1)
        self.assertEqual((self.d.state.num_reqs, self.d.state.total_sum), (1, 7))

    def test_dead_participant_does_not_block_sync_forever(self):
        del self.bus.nodes[self.a.ip]
        del self.bus.nodes[self.c.ip]
        self.d.view = [self.b.ip, self.c.ip, self.d.ip]
        self.d.promote(self.d.state.snapshot())
        self.pump(lambda: self.d.ready)
        self.assertEqual(set(self.d.view), {self.b.ip, self.d.ip})
        self.assertEqual(self.b.leader, self.d.ip)
        self.assertEqual(self.bus.submit(self.d, 1).total_sum, 7)

    def test_follower_does_not_rollback_a_newer_state_on_snapshot(self):
        self.seed(self.b, 1, 7)
        before = self.b.state.snapshot()
        self.b.handle(Message('SNAP', epoch=self.a.epoch, term=self.a.term,
                              members=self.a.view, snap=self.a.state.snapshot()), (self.a.ip, 4000))
        self.assertEqual(self.b.state.snapshot(), before)
        self.bus.drain()
        self.assertEqual(self.a.state.num_reqs, 1)

    def test_periodic_view_repairs_last_operation_lost_by_one_replica(self):
        # Simulate an existing lagging snapshot; no subsequent OP arrives at d.
        for node in (self.a, self.b, self.c):
            self.seed(node, 1, 7)
        self.a.announce()
        self.bus.drain()
        self.assertEqual(self.d.state.snapshot(), self.a.state.snapshot())
