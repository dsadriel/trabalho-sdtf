import socket
from time import monotonic as CLOCK

from common.config import MULTICAST_GROUP, BUFFER_SIZE, TIMEOUT_SECS
from common.protocol import Message, MessageType
from common.utils import format_timestamp


def discover_server(sock: socket.socket, port: int) -> tuple[str, int]:
    """Discover the current leader through multicast, retrying until it replies."""
    while True:
        sock.sendto(Message.discovery().encode(), (MULTICAST_GROUP, port))
        deadline = CLOCK() + TIMEOUT_SECS
        while True:
            remaining = deadline - CLOCK()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            try:
                packet, peer = sock.recvfrom(BUFFER_SIZE)
                response = Message.decode(packet)
            except (TimeoutError, ConnectionResetError):
                break
            except ValueError:
                continue
            if response.type == MessageType.DISCOVERY_RESP and peer[1] == port:
                print(format_timestamp(), 'server_addr', peer[0], flush=True)
                return peer
