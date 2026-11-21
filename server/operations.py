"""Independent records for operations waiting for replica confirmations."""
from dataclasses import dataclass, field


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
        return self.members.intersection(view).issubset(self.acks)

    def acknowledge(self, source, epoch, sequence, total, view):
        if (self.cancelled or epoch != self.epoch or sequence != self.sequence
                or total != self.expected_total or source not in self.members or source not in view):
            return False
        self.acks.add(source)
        return True
