import socket

from common.protocol import Message
from server.interface import print_server_duplicate, print_server_request
from server.state import ServerState


def handle_request(
    sock: socket.socket,
    addr: tuple[str, int],
    msg: Message,
    state: ServerState,
    show_port: bool = False,
) -> None:
    """Processes an incoming REQUEST message and sends back an ACK."""
    client_ip, client_port = addr

    status, num_reqs, total_sum = state.process_request(
        address=client_ip,
        id_req=msg.id_req,
        value=msg.value,
        port=client_port,
    )

    client_display = f"{client_ip}:{client_port}" if show_port else client_ip

    # Format output using server interface subservicing
    if status == "DUPLICATE":
        # Reply before logging: stdout may block, but the client must be
        # acknowledged as soon as state processing completes.
        ack = Message.ack(id_req=msg.id_req, num_reqs=num_reqs, total_sum=total_sum)
        sock.sendto(ack.encode(), addr)
        print_server_duplicate(
            client_display=client_display,
            id_req=msg.id_req,
            value=msg.value,
            num_reqs=num_reqs,
            total_sum=total_sum,
        )

    elif status == "GAP":
        # Gap format (id_req > last_req + 1): intermediate packet was lost.
        # Respond with ACK containing the LAST processed id_req so the client knows where it stopped.
        client = state.get_client(client_ip, client_port)
        last_req = client.last_req if client else 0
        ack = Message.ack(id_req=last_req, num_reqs=num_reqs, total_sum=total_sum)
        sock.sendto(ack.encode(), addr)

    else:
        ack = Message.ack(id_req=msg.id_req, num_reqs=num_reqs, total_sum=total_sum)
        sock.sendto(ack.encode(), addr)
        print_server_request(
            client_display=client_display,
            id_req=msg.id_req,
            value=msg.value,
            num_reqs=num_reqs,
            total_sum=total_sum,
        )


def handle_exit(
    sock: socket.socket,
    addr: tuple[str, int],
    state: ServerState,
    show_port: bool = False,
) -> None:
    """Handles client departure notification (EXIT).

    Keeps participant record in ServerState to preserve historical audit data
    (Table 1 of specification) and maintain idempotency against delayed UDP packets.
    """
    pass
