import socket
import sys

from common.config import BUFFER_SIZE
from common.protocol import Message, MessageType
from common.utils import parse_port
from server.discovery import handle_discovery
from server.interface import print_server_startup
from server.processing import handle_exit, handle_request
from server.state import ServerState


def main() -> None:
    args = sys.argv[1:]
    show_port = False

    # Check for optional flag --show-port / -p
    if "--show-port" in args:
        show_port = True
        args.remove("--show-port")
    elif "-p" in args:
        show_port = True
        args.remove("-p")

    # Validates the port argument
    if len(args) != 1:
        print(f"Usage: python {sys.argv[0]} <port> [--show-port]", file=sys.stderr)
        sys.exit(1)

    try:
        network_port = parse_port(args[0])
    except ValueError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)

    # Initialize shared server state
    state = ServerState()

    # Open UDP socket bound to all network interfaces on the specified port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", network_port))

    # Required output format upon startup: <data> num_reqs 0 total_sum 0
    print_server_startup(state.num_reqs, state.total_sum)

    try:
        while True:
            data, client_addr = sock.recvfrom(BUFFER_SIZE)
            try:
                msg = Message.decode(data)
            except ValueError:
                # Ignore invalid packets
                continue

            if msg.type == MessageType.DISCOVERY:
                handle_discovery(sock, client_addr, state)
            elif msg.type == MessageType.REQUEST:
                handle_request(sock, client_addr, msg, state, show_port=show_port)
            elif msg.type == MessageType.EXIT:
                handle_exit(sock, client_addr, state, show_port=show_port)

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()
