import socket

from common.config import BUFFER_SIZE, DISCOVERY_TIMEOUT_SECS
from common.protocol import Message, MessageType
from common.utils import format_timestamp


def discover_server(
    sock: socket.socket, port: int, broadcast_ip: str = "255.255.255.255"
) -> tuple[str, int]:
    """Actively discovers the server via UDP broadcast.

    Enables SO_BROADCAST on the socket, broadcasts DISCOVERY messages
    with periodic timeout/retries until a DISCOVERY_RESP is received.
    Prints the required discovery message and returns the server's (ip, port).
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(DISCOVERY_TIMEOUT_SECS)

    discovery_msg = Message.discovery().encode()
    broadcast_target = (broadcast_ip, port)

    while True:
        try:
            # Broadcast the discovery request
            sock.sendto(discovery_msg, broadcast_target)

            # Wait for unicast response from server
            data, server_addr = sock.recvfrom(BUFFER_SIZE)
            message = Message.decode(data)

            if message.type == MessageType.DISCOVERY_RESP:
                server_ip, server_port = server_addr
                # Format required by specification: <data> server_addr <ip>
                print(f"{format_timestamp()} server_addr {server_ip}")
                # Reset timeout back to default/None so subsequent calls manage their own
                sock.settimeout(None)
                return server_addr

        except (socket.timeout, TimeoutError):
            # Timed out waiting for response, loop and broadcast again
            continue
        except ValueError:
            # Ignore malformed packets that might arrive on the socket
            continue
