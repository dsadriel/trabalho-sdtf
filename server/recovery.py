"""Snapshot exchange and the readiness barrier after an election."""
import ipaddress

from server.quorum import required


def request_snapshot(self):
    self.send(self.leader, 'JOIN', snap=self.state.snapshot())


def follow(self, source, epoch, term, members):
    """Recognize a newer coordinator before asking it for a snapshot."""
    self.leader = self.coordinator = source
    self.epoch, self.term = epoch, term
    self.ready = False
    self.view = list(members)
    self.ballot = self.election_deadline = None
    self.hop.clear()
    self.tokens.clear()
    self.cancel_work()
    self.awaiting_reports.clear()
    self.awaiting_ready.clear()
    for monitor in self.monitors.values():
        monitor.cancel()
    with self.cv:
        self.cv.notify_all()
    self.interface.log('COORDINATOR recognized', source, 'term', term,
                       'local_seq', self.state.num_reqs, 'synchronizing')


def send_snapshot(self, destination):
    self.send(destination, 'SNAP', snap=self.state.snapshot(), epoch=self.epoch,
              term=self.term, members=self.view)


def finish_sync(self):
    if self.leader != self.ip or self.ready or self.awaiting_reports or self.awaiting_ready:
        return
    self.ready = True
    self.interface.log('COORDINATOR ready', self.ip, 'term', self.term,
                       'seq', self.state.num_reqs, 'sum', self.state.total_sum)
    with self.cv:
        self.cv.notify_all()
    self.announce()


def handle_join(self, m, addr):
    source = addr[0]
    if self.leader != self.ip or source == self.ip:
        return
    if source not in self.view:
        # RM novato: adiciona em join_waiters para pausar novas admissões até o pipeline esvaziar.
        self.join_waiters.add(source)
    snapshot = m.payload.get('snap')
    if (source in self.view or source in self.monitors) and snapshot is not None:
        if snapshot[0] == self.state.num_reqs and snapshot[1] != self.state.total_sum:
            self.interface.log('STATE CONFLICT', source, 'seq', snapshot[0])
            self.ready = False
            self.awaiting_reports.add(source)
            return
        if snapshot[0] > self.state.num_reqs:
            if self.pending:
                return  # Não sobrescreve enquanto há op em voo (o RM pode já tê-la aplicado).
            # Adota o estado mais avançado reportado por um sobrevivente e exige novo READY de todos.
            self.state.restore(snapshot)
            self.cancel_work()
            self.ready = False
            self.awaiting_ready = set(self.view) - {self.ip}
            self.interface.log('RECOVERY newer state', source, 'seq', snapshot[0])
        self.awaiting_reports.discard(source)
    # Aguarda todos os sobreviventes conhecidos reportarem antes de distribuir o SNAP consolidado.
    if self.awaiting_reports:
        return
    if not self.ready:
        for peer in self.awaiting_ready:
            send_snapshot(self, peer)
        if source not in self.awaiting_ready:
            send_snapshot(self, source)
        finish_sync(self)
    else:
        send_snapshot(self, source)


def handle_snap(self, m, addr):
    source = addr[0]
    epoch = m.payload['epoch']
    term = m.payload.get('term', self.term)
    if source == self.ip or self.leader not in (None, source):
        return
    if self.leader == source and (epoch != self.epoch or term != self.term):
        return  # Delayed snapshot from a previous leadership.
    if self.leader is None:
        rank = (term, int(ipaddress.IPv4Address(source)))
        if rank < self.leadership_rank() or (self.ballot is not None and rank == self.leadership_rank()):
            return
        follow(self, source, epoch, term, m.payload.get('members', [source, self.ip]))
    snapshot = m.payload['snap']
    if snapshot[0] < self.state.num_reqs:
        request_snapshot(self)  # Report the newer state instead of rolling back.
        return
    if snapshot[0] == self.state.num_reqs and snapshot[1] != self.state.total_sum:
        self.interface.log('STATE CONFLICT', source, 'seq', snapshot[0])
        return
    self.state.restore(snapshot)
    self.cancel_work()
    self.ready = True
    self.view = m.payload.get('members', self.view)
    self.send(source, 'READY', epoch=self.epoch, term=self.term, seq=self.state.num_reqs)


def handle_ready(self, m, addr):
    source = addr[0]
    if self.leader != self.ip or m.payload['epoch'] != self.epoch:
        return
    if m.payload.get('term', self.term) != self.term or m.payload['seq'] != self.state.num_reqs:
        return
    if source not in self.view:
        if self.pending:
            self.join_waiters.add(source)
            return
        self.view.append(source)
        self.join_waiters.discard(source)
        self.interface.log('RM joined', source, 'members', self.view,
                           'quorum', required(len(self.view) - 1))
        self.announce()
    self.join_waiters.discard(source)
    self.awaiting_ready.discard(source)
    self.cv.notify_all()
    finish_sync(self)


def resume_joins(self):
    """A short admission barrier drains the pipeline before adding new RMs."""
    if self.leader == self.ip and self.ready and not self.pending:
        for peer in self.join_waiters:
            send_snapshot(self, peer)
