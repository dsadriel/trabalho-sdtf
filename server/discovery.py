from common.protocol import Message
from server.state import ServerState


def handle_discovery(sock, addr: tuple[str, int], state: ServerState) -> None:
    """Register the client and reply; the dispatcher admits only the leader."""
    state.register_client(ip=addr[0], port=addr[1])
    sock.sendto(Message.discovery_response().encode(), addr)
