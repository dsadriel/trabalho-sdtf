from time import monotonic as CLOCK

from common.config import (
    MULTICAST_GROUP as GROUP, DISCOVERY_WINDOW, VIEW_INTERVAL, HEARTBEAT_FAILURES,
)
from server.quorum import required
from server import recovery, replication
from server.election import number


def monitored_peers(self):
    if self.leader == self.ip:
        return (set(self.view) - {self.ip}) | self.join_waiters
    return {self.leader} if self.leader else set()


def announcement_interval(self):
    pending = self.awaiting_reports | self.awaiting_ready | self.join_waiters
    return min([VIEW_INTERVAL] + [self.monitor_for(peer).timeout for peer in pending])


def next_timeout(self):
    """Time until the nearest pending deadline; never impose a fixed polling delay."""
    now = CLOCK()
    deadlines = [hop[4] for hop in self.hop.values()]
    for peer in monitored_peers(self):
        monitor = self.monitor_for(peer)
        deadlines.append(monitor.deadline if monitor.probe else monitor.next_probe)
    if self.leader == self.ip:
        deadlines.append(self.next_announce)
    elif self.leader is None:
        if self.ballot is not None:
            deadlines.append(self.election_deadline)
        else:
            deadlines.extend((self.next_join, self.started + DISCOVERY_WINDOW))
    return max(0.0, min(deadlines) - now) if deadlines else None


def tick(self):
    now = CLOCK()
    for uid, hop in list(self.hop.items()):
        if uid in self.hop and now >= hop[4]:
            self.retry(uid)

    if self.leader == self.ip:
        if now >= self.next_announce:
            self.announce()
            self.next_announce = now + announcement_interval(self)
    elif self.leader is None:
        if self.ballot is not None:
            if now >= self.election_deadline:
                self.elect()
        else:
            if now >= self.next_join:
                self.send(GROUP, 'JOIN', snap=self.state.snapshot())
                self.next_join = now + VIEW_INTERVAL
            if now - self.started >= DISCOVERY_WINDOW:
                self.promote(self.state.snapshot())

    peers = monitored_peers(self)
    for peer, monitor in self.monitors.items():
        if peer not in peers:
            monitor.cancel()
    for peer in sorted(peers):
        monitor = self.monitor_for(peer)
        if monitor.probe is not None and now >= monitor.deadline:
            # Count one actual unanswered probe, even after a long scheduler pause.
            monitor.probe = None
            monitor.failures += 1
            monitor.next_probe = now
            rtt_ms = 'unmeasured' if monitor.average_rtt is None else round(monitor.average_rtt * 1000, 3)
            self.interface.log('HEARTBEAT timeout', peer, 'consecutive', monitor.failures,
                               'timeout_ms', round((monitor.deadline - monitor.sent) * 1000, 3),
                               'rtt_ms', rtt_ms)
            if monitor.failures >= HEARTBEAT_FAILURES:
                if self.leader == self.ip:
                    if peer in self.view:
                        self.view.remove(peer)
                    self.join_waiters.discard(peer)
                    self.awaiting_reports.discard(peer)
                    self.awaiting_ready.discard(peer)
                    if not self.awaiting_reports:
                        for waiting in self.awaiting_ready:
                            recovery.send_snapshot(self, waiting)
                    recovery.finish_sync(self)
                    self.interface.log('RM timeout', peer, 'members', self.view,
                                       'quorum', required(len(self.view) - 1))
                    with self.cv:
                        self.cv.notify_all()
                    self.announce()
                    monitor.cancel()
                    continue
                self.elect()
                break
        if monitor.probe is None and now >= monitor.next_probe:
            probe = monitor.start(now)
            self.send(peer, 'HEARTBEAT', epoch=self.epoch, probe=probe)


def handle_heartbeat(self, message, addr):
    source = addr[0]
    if source == self.ip or source not in self.view or message.payload['epoch'] != self.epoch:
        return
    probe = message.payload.get('probe')
    if isinstance(probe, str):
        self.send(source, 'ALIVE', epoch=self.epoch, probe=probe)


def handle_alive(self, message, addr):
    source = addr[0]
    if source not in monitored_peers(self) or message.payload['epoch'] != self.epoch:
        return
    self.monitor_for(source).acknowledge(message.payload.get('probe'), CLOCK())


def handle_view(self, m, addr):
    source = addr[0]
    if source == self.ip:
        return
    fields = m.payload
    term = fields.get('term', self.term)
    rank = (term, number(source))
    current = self.leadership_rank()
    if rank < current:
        return
    if rank > current:
        # A known participant may announce the election even before our own timeout.
        if (self.view and source not in self.view and source not in self.monitors
                and self.leader not in (None, source)):
            return
        recovery.follow(self, source, fields['epoch'], term, fields['members'])
    elif source != self.leader or fields['epoch'] != self.epoch:
        return
    self.view = fields['members']
    replication.discard_results(self, fields.get('seq', 0))
    if (not self.ready or self.ip not in self.view or fields.get('syncing', False)
            or fields.get('seq', self.state.num_reqs) > self.state.num_reqs):
        recovery.request_snapshot(self)
