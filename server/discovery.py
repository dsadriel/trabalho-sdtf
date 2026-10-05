import socket

from common.protocol import Message
from server.state import ServerState


def handle_discovery(
    sock: socket.socket, addr: tuple[str, int], state: ServerState
) -> None:
    """Handles an incoming discovery request from a client.

    Registers the client in the server state and replies in unicast
    with a DISCOVERY_RESP message.
    """
    client_ip, client_port = addr
    state.register_client(ip=client_ip, port=client_port)

    response = Message.discovery_response()
    sock.sendto(response.encode(), addr)
