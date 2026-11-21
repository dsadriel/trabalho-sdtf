import argparse
import socket
import sys

from client.discovery import discover_server
from client.interface import run_client_interface
from client.processing import HistoryGapError
from common.config import TIMEOUT_SECS
from common.utils import parse_port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=parse_port)
    parser.add_argument('--local', action='store_true')
    args = parser.parse_args()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        if args.local:
            sock.bind(('127.0.0.1', 0))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF,
                            socket.inet_aton('127.0.0.1'))
        sock.settimeout(TIMEOUT_SECS)
        try:
            server_addr = discover_server(sock, args.port)
            run_client_interface(sock, server_addr)
        except HistoryGapError as exc:
            print(f'ERRO: {exc}', file=sys.stderr, flush=True)
            raise SystemExit(1)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
