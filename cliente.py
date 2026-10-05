import socket
import sys

from client.discovery import discover_server
from client.interface import run_client_interface
from common.utils import parse_port


def main() -> None:
    # Validates the user input
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <port>", file=sys.stderr)
        sys.exit(1)

    try:
        network_port = parse_port(sys.argv[1])
    except ValueError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)

    # Create UDP client socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        # Discover the server via broadcast
        server_addr = discover_server(sock, network_port)

        # Run multithreaded interface (dedicated reader and writer threads)
        run_client_interface(sock, server_addr)

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()