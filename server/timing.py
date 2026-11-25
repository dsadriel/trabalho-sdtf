"""Per-peer timing measured on a local monotonic clock, in seconds."""
import math
import uuid

from common.config import RTT_ALPHA, RTT_INITIAL_TIMEOUT, RTT_MIN_TIMEOUT, RTT_MULTIPLIER


class PeerMonitor:
    def __init__(self, initial=RTT_INITIAL_TIMEOUT, minimum=RTT_MIN_TIMEOUT):
        self.initial = initial
        self.minimum = minimum
        self.average_rtt = None
        self.probe = None
        self.sent = 0.0
        self.deadline = 0.0
        self.next_probe = 0.0
        self.failures = 0

    @property
    def timeout(self):
        value = self.initial if self.average_rtt is None else RTT_MULTIPLIER * self.average_rtt
        return max(self.minimum, value)

    def observe(self, sample):
        """Atualiza a média móvel exponencial (EWMA) do RTT: peso alpha=0.125 para a nova amostra."""
        if not math.isfinite(sample) or sample < 0:
            return
        if self.average_rtt is None:
            self.average_rtt = sample
        else:
            self.average_rtt += RTT_ALPHA * (sample - self.average_rtt)

    def start(self, now):
        self.probe = uuid.uuid4().hex
        self.sent = now
        self.deadline = now + self.timeout
        return self.probe

    def acknowledge(self, probe, now):
        # Each request has its own ID; late/duplicate replies cannot reset losses.
        if self.probe is None or probe != self.probe or now > self.deadline:
            return False
        self.observe(now - self.sent)
        self.probe = None
        self.failures = 0
        self.next_probe = now + self.timeout
        return True

    def cancel(self):
        """Stop monitoring on a role/view change, retaining the peer's RTT estimate."""
        self.probe = None
        self.failures = 0
        self.next_probe = 0.0
