import ipaddress
import uuid
from time import monotonic as CLOCK



def number(ip):
    return int(ipaddress.IPv4Address(ip))


def forward(self, token):
    ring = token['ring']
    pos = ring.index(self.ip)
    peers = (ring[pos+1:] + ring[:pos])
    self.hop[token['uid']] = [token, peers, 0, 0, 0]
    self.retry(token['uid'])


def retry(self, uid):
    token, peers, i, _, _ = self.hop[uid]
    if i >= len(peers):
        self.hop.pop(uid)
        if self.ballot and token['path'] == [self.ip]:
            self.view = [self.ip]
            self.promote(self.state.snapshot())
        return
    dest = peers[i]
    self.interface.log('ELECTION forward', self.ip, '->', dest)
    now = CLOCK()
    self.hop[uid][2:] = [i+1, now, now + self.monitor_for(dest).timeout]
    self.send(dest, 'TOKEN', token=token)


def elect(self):
    ring = sorted(set(self.view) - {self.leader} | {self.ip}, key=number)
    self.interface.log('FE timeout', self.leader, 'ELECTION start', self.ip)
    self.view = ring
    self.leader = None
    self.ready = False
    self.cancel_work()
    self.ballot = CLOCK()
    self.election_deadline = self.ballot + 2 * sum(
        self.monitor_for(peer).timeout for peer in ring
    )
    if len(ring) == 1:
        self.promote(self.state.snapshot())
        return
    self.forward(dict(uid=uuid.uuid4().hex, ring=ring, path=[self.ip],
                      snap=self.state.snapshot(), epoch=self.epoch, term=self.term + 1))


def handle_token(self, m, addr):
    src = addr[0]
    t=m.payload['token']
    if t['epoch']!=self.epoch or self.ip not in t['ring']:
        return
    self.send(src,'HOP_ACK',uid=t['uid'],epoch=self.epoch)
    if self.ballot is None and self.leader != self.ip:
        # Receiving a valid election token also makes this node a participant.
        self.leader = None
        self.ready = False
        self.cancel_work()
        self.view = list(t['ring'])
        self.ballot = CLOCK()
        self.election_deadline = self.ballot + 2 * sum(
            self.monitor_for(peer).timeout for peer in t['ring'])
    self.interface.log('ELECTION received', src, 'at',self.ip)
    if self.ip in t['path']:
        winner=max(t['path'],key=number)
        self.send(winner,'WIN',epoch=self.epoch,snap=t['snap'],members=t['path'],term=t.get('term',self.term + 1))
    elif t['uid'] not in self.tokens:
        self.tokens.add(t['uid'])
        t['path'].append(self.ip)
        if self.state.num_reqs>t['snap'][0]:
            t['snap']=self.state.snapshot()
        self.forward(t)


def handle_hop_ack(self, m, addr):
    hop = self.hop.get(m.payload['uid'])
    if hop is None or m.payload.get('epoch') != self.epoch:
        return
    _, peers, index, sent, deadline = hop
    now = CLOCK()
    if index and addr[0] == peers[index - 1] and now <= deadline:
        self.monitor_for(addr[0]).observe(now - sent)
        self.hop.pop(m.payload['uid'])


def handle_win(self, m, addr):
    if not (m.payload['epoch'] == self.epoch):
        return
    members = m.payload['members']
    term = m.payload.get('term', self.term + 1)
    if not members or max(members, key=number) != self.ip or term <= self.term:
        return
    self.view = members
    self.promote(m.payload['snap'], term=term)
