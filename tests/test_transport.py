import socket
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from common.config import MULTICAST_GROUP
from server.transport import open_sockets


class TestTransport(unittest.TestCase):
    def check_multicast_binding(self, platform, expected_address):
        unicast, multicast = Mock(), Mock()
        unicast.getsockname.return_value = ('127.0.0.1', 4000)
        node = SimpleNamespace()
        with patch('server.transport.sys.platform', platform), patch(
            'server.transport.socket.socket', side_effect=[unicast, multicast]
        ):
            open_sockets(node, 4000, True)
        multicast.bind.assert_called_once_with((expected_address, 4000))
        multicast.setsockopt.assert_any_call(
            socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton('127.0.0.1'),
        )

    def test_windows_binds_local_wildcard_before_joining_group(self):
        self.check_multicast_binding('win32', '')

    def test_linux_keeps_multicast_binding_separate_from_unicast(self):
        self.check_multicast_binding('linux', MULTICAST_GROUP)
