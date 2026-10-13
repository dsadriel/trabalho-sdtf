# Port constraints
MIN_PORT = 1
MAX_PORT = 65535

# Socket & Networking
BUFFER_SIZE = 1024

# Timeouts & Retries
TIMEOUT_SECS = 0.5  # Changed from 0.02s to 0.5s due to too many DUP!!s
DISCOVERY_TIMEOUT_SECS = 1.0  # Timeout when waiting for discovery broadcast response
MAX_RETRIES = 10