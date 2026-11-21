"""UDP sockets and the single receive loop."""
import select
import socket
import sys

from common.config import MULTICAST_GROUP as GROUP, BUFFER_SIZE
from common.protocol import Message


def open_sockets(self, port, local):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if local:
        for i in range(1, 255):
            try:
                self.sock.bind((f'127.0.0.{i}', port))
                break
            except OSError:
                if i == 254:
                    raise
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect((GROUP, port))
            self.sock.bind((probe.getsockname()[0], port))
    self.ip = self.sock.getsockname()[0]
    iface = socket.inet_aton('127.0.0.1' if local else self.ip)
    self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, iface)
    self.group = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.group.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Winsock requires a local address for bind; subscribe to GROUP below.
    # Linux can bind to GROUP, keeping multicast separate from unicast traffic.
    self.group.bind(('' if sys.platform == 'win32' else GROUP, port))
    self.group.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                          socket.inet_aton(GROUP) + iface)


def send(self, dest, kind, **fields):
    addr = dest if isinstance(dest, tuple) else (dest, self.port)
    self.sock.sendto(Message(kind, **fields).encode(), addr)


def run(self):
    while True:
        with self.cv:
            self.tick()
            timeout = self.next_timeout()
        ready,_,_=select.select([self.sock,self.group],[],[],timeout)
        with self.cv:
            for s in ready:
                try:
                    data,addr=s.recvfrom(BUFFER_SIZE)
                except ConnectionResetError:
                    # Windows may surface an ICMP port-unreachable here after a crash.
                    # The unanswered heartbeat still expires through its normal timer.
                    continue
                try:
                    self.handle(Message.decode(data),addr)
                except (ValueError,KeyError,TypeError) as exc:
                    self.interface.log('invalid datagram',exc)
            self.tick()
