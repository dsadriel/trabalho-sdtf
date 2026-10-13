import socket

from common.config import BUFFER_SIZE, TIMEOUT_SECS
from common.protocol import Message, MessageType


def send_request_and_wait_ack(
    sock: socket.socket, server_addr: tuple[str, int], id_req: int, value: int
) -> Message:
    """Sends a REQUEST and waits for an ACK with retransmissions on timeout."""
    req_msg = Message.request(id_req=id_req, value=value).encode()
    sock.settimeout(TIMEOUT_SECS)

    while True:
        try:
            sock.sendto(req_msg, server_addr)
            data, from_addr = sock.recvfrom(BUFFER_SIZE)
            msg = Message.decode(data)

            if msg.type == MessageType.ACK and msg.id_req == id_req:
                return msg

        except (socket.timeout, TimeoutError):
            # Timeout expired, retransmit the request]
            continue
        except ValueError:
            # Ignore invalid packets
            continue
