import socket
import unittest

from common.protocol import Message, MessageType
from server.discovery import handle_discovery
from server.state import ServerState


class TestDiscovery(unittest.TestCase):
    def test_discovery_handshake(self):
        # Setup mock server socket on loopback
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_sock.bind(("127.0.0.1", 0))
        server_port = server_sock.getsockname()[1]

        # Setup mock client socket
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        server_state = ServerState()

        # Send discovery directly to server port (simulating loopback broadcast)
        disc_msg = Message.discovery().encode()
        client_sock.sendto(disc_msg, ("127.0.0.1", server_port))

        # Server receives and handles discovery
        data, client_addr = server_sock.recvfrom(1024)
        handle_discovery(server_sock, client_addr, server_state)

        # Check client registered in server state
        registered = server_state.get_client(ip=client_addr[0], port=client_addr[1])
        self.assertIsNotNone(registered)
        self.assertEqual(registered.ip, client_addr[0])

        # Client receives response
        resp_data, resp_addr = client_sock.recvfrom(1024)
        resp_msg = Message.decode(resp_data)
        self.assertEqual(resp_msg.type, MessageType.DISCOVERY_RESP)
        self.assertEqual(resp_addr, ("127.0.0.1", server_port))

        server_sock.close()
        client_sock.close()


if __name__ == "__main__":
    unittest.main()
