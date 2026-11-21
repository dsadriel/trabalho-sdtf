import socket
from time import monotonic as CLOCK

from client.discovery import discover_server
from common.config import BUFFER_SIZE, TIMEOUT_SECS
from common.protocol import Message, MessageType


class HistoryGapError(RuntimeError):
    """The contacted coordinator cannot safely continue this client's history."""

    def __init__(self, server, requested, last_request, sequence):
        self.server = server
        self.requested = requested
        self.last_request = last_request
        self.sequence = sequence
        super().__init__(
            f'Histórico incompleto no servidor {server[0]}:{server[1]}: '
            f'pedido {requested}, último conhecido {last_request}, sequência global {sequence}. '
            'Envio interrompido para não apresentar uma soma incompleta como correta.')


def send_request_and_wait_ack(sock: socket.socket, server_addr: tuple[str, int],
                              id_req: int, value: int, *, session: str):
    """Retry the same Part 1 request, rediscovering the leader on timeout."""
    packet = Message.request(id_req, value, session=session).encode()
    port = server_addr[1]
    while True:
        sock.sendto(packet, server_addr)
        deadline = CLOCK() + TIMEOUT_SECS
        while True:
            remaining = deadline - CLOCK()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            try:
                data, peer = sock.recvfrom(BUFFER_SIZE)
                ack = Message.decode(data)
            except (TimeoutError, ConnectionResetError):
                break
            except ValueError:
                continue
            if (peer == server_addr and ack.type == MessageType.HISTORY_GAP
                    and ack.payload.get('session') == session
                    and ack.payload.get('id_req') == id_req
                    and type(ack.payload.get('last_req')) is int
                    and type(ack.payload.get('num_reqs')) is int):
                raise HistoryGapError(server_addr, id_req, ack.payload['last_req'], ack.num_reqs)
            if peer == server_addr and ack.type == MessageType.ACK and ack.id_req == id_req:
                return ack, server_addr
        server_addr = discover_server(sock, port)
