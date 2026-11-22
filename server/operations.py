"""Independent records for operations waiting for replica confirmations."""
from dataclasses import dataclass, field

from server.quorum import required


@dataclass
class PendingOperation:
    epoch: str
    sequence: int
    session: str | None
    request_id: int
    value: int
    address: tuple[str, int]
    expected_total: int
    members: frozenset[str]
    acks: set[str] = field(default_factory=set)
    cancelled: bool = False

    @property
    def key(self):
        return self.epoch, self.sequence

    @property
    def packet(self):
        return [self.sequence, self.session, self.request_id, self.value, *self.address]

    def confirmed(self, view):
        active_participants = self.members.intersection(view)
        if not active_participants:
            return False
        active_acks = self.acks.intersection(active_participants)
        return len(active_acks) >= required(len(active_participants))

    def acknowledge(self, source, epoch, sequence, total, view):
        if (self.cancelled or epoch != self.epoch or sequence != self.sequence
                or total != self.expected_total or source not in self.members or source not in view):
            return False
        self.acks.add(source)
        return True
