import threading
import uuid
from time import monotonic as CLOCK

from common.config import MULTICAST_GROUP as GROUP
from common.protocol import MessageType
from server.interface import ServerInterface
from server.state import ServerState
from server.timing import PeerMonitor
from server import discovery, heartbeat, processing, quorum, recovery, replication, transport

from server import election


HANDLERS = {
    'JOIN': recovery.handle_join,
    'SNAP': recovery.handle_snap,
    'READY': recovery.handle_ready,
    'VIEW': heartbeat.handle_view,
    'ALIVE': heartbeat.handle_alive,
    'HEARTBEAT': heartbeat.handle_heartbeat,
    'OP': replication.handle_op,
    'RESULT': quorum.handle_result,
    'TOKEN': election.handle_token,
    'HOP_ACK': election.handle_hop_ack,
    'WIN': election.handle_win,
}


class Node:
    """Owns the state and coordinates the protocol handlers."""

    def __init__(self, port, local, show_port=False):
        self.port = port
        transport.open_sockets(self, port, local)
        self.cv = threading.Condition()
        self.leader = None
        # Retain the accepted coordinator's rank while leader is None during election.
        self.coordinator = None
        self.term = 0
        self.ready = False
        self.awaiting_reports = set()
        self.awaiting_ready = set()
        self.view = []
        self.epoch = ''
        self.monitors = {}
        self.state = ServerState()
        self.interface = ServerInterface()
        self.show_port = show_port
        self.buffer = {}
        self.pending = {}
        self.request_workers = {}
        self.next_sequence = 0
        self.reserved_total = 0
        self.join_waiters = set()
        self.result_cache = {}
        self.replica_jobs = set()
        self.replica_version = 0
        self.replica_committed = 0
        self.ballot = None
        self.election_deadline = None
        self.tokens = set()
        self.hop = {}
        self.started = CLOCK()
        self.next_announce = self.started
        self.next_join = self.started

    def announce(self):
        fields = dict(epoch=self.epoch, term=self.term, members=self.view,
                      seq=self.state.num_reqs, syncing=not self.ready)
        self.send(GROUP, 'VIEW', **fields)
        # Repeat the transition directly to peers until they confirm the snapshot.
        for peer in self.awaiting_reports | self.awaiting_ready | self.join_waiters:
            self.send(peer, 'VIEW', **fields)

    def leadership_rank(self):
        return self.term, election.number(self.coordinator) if self.coordinator else -1

    def monitor_for(self, peer):
        if peer not in self.monitors:
            self.monitors[peer] = PeerMonitor()
        return self.monitors[peer]

    def cancel_work(self):
        with self.cv:
            for operation in self.pending.values():
                operation.cancelled = True
            self.pending.clear()
            self.request_workers.clear()
            self.buffer.clear()
            self.result_cache.clear()
            self.replica_version += 1
            self.replica_committed = self.state.num_reqs
            self.next_sequence, self.reserved_total = self.state.num_reqs, self.state.total_sum
            self.join_waiters.clear()
            self.cv.notify_all()

    def elect(self):
        with self.cv:
            election.elect(self)

    def tick(self):
        with self.cv:
            heartbeat.tick(self)

    def promote(self, snap, term=None):
        with self.cv:
            # Never replace a more recent local state by an older election token.
            if snap[0] > self.state.num_reqs:
                self.state.restore(snap)
            elif snap[0] == self.state.num_reqs and snap[1] != self.state.total_sum:
                raise ValueError('conflicting election snapshots at the same sequence')
            self.cancel_work()
            self.leader = self.coordinator = self.ip
            self.term = self.term + 1 if term is None else term
            self.epoch = uuid.uuid4().hex
            self.ready = False
            self.ballot = self.election_deadline = None
            self.hop.clear()
            self.tokens.clear()
            self.view = sorted(set(self.view + [self.ip]), key=election.number)
            self.awaiting_reports = set(self.view) - {self.ip}
            self.awaiting_ready = self.awaiting_reports.copy()
            for monitor in self.monitors.values():
                monitor.cancel()
            self.interface.log('COORDINATOR', self.ip, 'members', self.view,
                               'term', self.term, 'seq', self.state.num_reqs, 'synchronizing')
            self.announce()
            recovery.finish_sync(self)

    def handle(self, message, address):
        with self.cv:
            # Preserve Part 1 entry points; only the leader serves client messages.
            if message.type in (MessageType.DISCOVERY, MessageType.REQUEST, MessageType.EXIT):
                if self.leader != self.ip or not self.ready:
                    return
                if message.type == MessageType.DISCOVERY:
                    discovery.handle_discovery(self.sock, address, self.state)
                elif message.type == MessageType.EXIT:
                    processing.handle_exit(self.sock, address, self.state,
                                           self.interface, self.show_port)
                else:
                    processing.start_request(self, message, address)
                return
            handler = HANDLERS.get(message.type)
            if handler is not None:
                handler(self, message, address)

    send = transport.send
    run = transport.run
    forward = election.forward
    retry = election.retry
    next_timeout = heartbeat.next_timeout
