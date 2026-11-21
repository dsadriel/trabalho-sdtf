# Port constraints
MIN_PORT = 1
MAX_PORT = 65535

# Socket & Networking
BUFFER_SIZE = 65507  # Maximum IPv4 UDP payload, including state snapshots

# Timeouts & Retries
TIMEOUT_SECS = 0.5  # Maximum client response wait; valid ACKs return immediately


MULTICAST_GROUP = '239.192.85.2'
RTT_INITIAL_TIMEOUT = 0.1  # Before the first valid sample (seconds)
RTT_MULTIPLIER = 3.0
RTT_ALPHA = 0.125  # EWMA: weight of each new RTT sample
RTT_MIN_TIMEOUT = 0.05  # Configurable scheduler safety floor, shared by Linux and Windows
HEARTBEAT_FAILURES = 3
VIEW_INTERVAL = 0.3  # Membership announcements, independent of heartbeat deadlines
DISCOVERY_WINDOW = 1.5

# Concurrency/admission limits, independently tunable from failure detection.
MAX_INFLIGHT_OPERATIONS = 64
OP_RETRY_INTERVAL = 0.2
