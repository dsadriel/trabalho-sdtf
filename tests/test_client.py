import io
import collections
import queue
import threading
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from client.interface import user_input_reader
from client.processing import HistoryGapError, send_request_and_wait_ack
from common.protocol import Message


class TestClient(unittest.TestCase):
    def test_cli_prints_history_error_and_exits_without_traceback(self):
        import cliente
        failure = HistoryGapError(('127.0.0.4', 4000), 1322, 308, 10308)
        with patch('cliente.sys.argv', ['cliente.py', '4000']), patch(
            'cliente.socket.socket', return_value=MagicMock()
        ), patch('cliente.discover_server', return_value=('127.0.0.4', 4000)), patch(
            'cliente.run_client_interface', side_effect=failure
        ), patch('cliente.sys.stderr', new_callable=io.StringIO) as output:
            with self.assertRaises(SystemExit) as exited:
                cliente.main()
        self.assertEqual(exited.exception.code, 1)
        self.assertIn('Histórico incompleto', output.getvalue())
        self.assertNotIn('Traceback', output.getvalue())

    def test_missing_history_is_explicit_instead_of_rediscovery_forever(self):
        server = ('127.0.0.4', 4000)
        sock = Mock()
        sock.recvfrom.return_value = (Message(
            'HISTORY_GAP', session='a' * 32, id_req=1322, last_req=308,
            num_reqs=10308, total_sum=264398).encode(), server)
        with patch('client.processing.discover_server') as discover:
            with self.assertRaises(HistoryGapError) as error:
                send_request_and_wait_ack(sock, server, 1322, 12, session='a' * 32)
        self.assertEqual(error.exception.last_request, 308)
        self.assertEqual(error.exception.sequence, 10308)
        self.assertIn('Histórico incompleto', str(error.exception))
        discover.assert_not_called()
        self.assertEqual(sock.sendto.call_count, 1)

    def test_stale_history_error_from_another_request_is_ignored(self):
        server = ('127.0.0.4', 4000)
        sock = Mock()
        sock.recvfrom.side_effect = [
            (Message('HISTORY_GAP', session='old-session', id_req=2,
                     last_req=0, num_reqs=0).encode(), server),
            (Message.ack(2, 2, 14).encode(), server),
        ]
        ack, _ = send_request_and_wait_ack(sock, server, 2, 7, session='a' * 32)
        self.assertEqual(ack.total_sum, 14)
        self.assertEqual(sock.sendto.call_count, 1)

    def test_sequential_requests_with_noise_do_not_generate_duplicate_processing(self):
        from test_cluster import Bus
        bus = Bus()
        leader, _, _ = bus.cluster()
        leader.interface.print_duplicate = Mock()
        server = (leader.ip, 4000)
        sock = Mock()
        noise = collections.deque()

        def send(data, address):
            self.assertEqual(address, server)
            request = Message.decode(data)
            noise.extend([
                Message.discovery_response().encode(),
                Message.ack(request.id_req - 1, 0, 0).encode(),
            ])
            with leader.cv:
                leader.handle(request, ('client', 4000))

        def receive(_):
            if noise:
                return noise.popleft(), server
            deadline = time.monotonic() + sock.settimeout.call_args.args[0]
            while time.monotonic() < deadline:
                bus.drain()
                if bus.replies:
                    return bus.replies.pop(0).encode(), server
                time.sleep(0.001)
            raise TimeoutError()

        sock.sendto.side_effect = send
        sock.recvfrom.side_effect = receive
        with patch('client.processing.discover_server', side_effect=AssertionError(
            'No packet was lost: rediscovery would be unnecessary'
        )):
            for request_id in range(1, 26):
                ack, _ = send_request_and_wait_ack(
                    sock, server, request_id, 7, session='a' * 32)
                self.assertEqual(ack.total_sum, request_id * 7)
        self.assertEqual(sock.sendto.call_count, 25)
        self.assertEqual(leader.state.num_reqs, 25)
        leader.interface.print_duplicate.assert_not_called()

    def test_unrelated_packets_do_not_extend_original_request_deadline(self):
        server = ('127.0.0.1', 4000)
        sock = Mock()
        now = [100.0]

        def receive(_):
            now[0] += 0.2
            return Message.discovery_response().encode(), server

        sock.recvfrom.side_effect = receive
        with patch('client.processing.CLOCK', side_effect=lambda: now[0]), patch(
            'client.processing.discover_server', side_effect=RuntimeError('deadline expired')
        ) as discover:
            with self.assertRaisesRegex(RuntimeError, 'deadline expired'):
                send_request_and_wait_ack(sock, server, 1, 7, session='a' * 32)
        discover.assert_called_once_with(sock, 4000)
        self.assertEqual(sock.sendto.call_count, 1)
        waits = [call.args[0] for call in sock.settimeout.call_args_list]
        self.assertAlmostEqual(waits[0], 0.5)
        self.assertAlmostEqual(waits[1], 0.3)
        self.assertAlmostEqual(waits[2], 0.1)

    def test_discovery_waits_through_unrelated_packets_without_rebroadcasting(self):
        from client.discovery import discover_server
        server = ('127.0.0.1', 4000)
        sock = Mock()
        sock.recvfrom.side_effect = [
            (Message.ack(1, 1, 7).encode(), server),
            (b'not-json', server),
            (Message.discovery_response().encode(), server),
        ]
        with patch('client.discovery.print'):
            self.assertEqual(discover_server(sock, 4000), server)
        self.assertEqual(sock.sendto.call_count, 1)

    def test_delayed_discovery_response_does_not_resend_request(self):
        server = ('127.0.0.1', 4000)
        sock = Mock()
        sock.recvfrom.side_effect = [
            (Message.discovery_response().encode(), server),
            (Message.ack(1, 1, 7).encode(), server),
        ]
        with patch('client.processing.discover_server', side_effect=AssertionError(
            'A packet that is not the requested ACK is not a timeout'
        )):
            ack, address = send_request_and_wait_ack(sock, server, 1, 7, session='a' * 32)
        self.assertEqual((ack.total_sum, address), (7, server))
        self.assertEqual(sock.sendto.call_count, 1)

    def test_old_or_foreign_acks_do_not_trigger_retransmission(self):
        server = ('127.0.0.1', 4000)
        sock = Mock()
        sock.recvfrom.side_effect = [
            (Message.ack(1, 1, 7).encode(), server),
            (Message.ack(2, 2, 14).encode(), ('127.0.0.2', 4000)),
            (b'not-json', server),
            (Message.ack(2, 2, 14).encode(), server),
        ]
        with patch('client.processing.discover_server', side_effect=AssertionError(
            'Keep waiting for the matching ACK until the deadline'
        )):
            ack, _ = send_request_and_wait_ack(sock, server, 2, 7, session='a' * 32)
        self.assertEqual(ack.id_req, 2)
        self.assertEqual(sock.sendto.call_count, 1)

    def test_timeout_rediscovers_leader_and_retries_identical_request(self):
        old = ('127.0.0.1', 4000)
        new = ('127.0.0.3', 4000)
        sock = Mock()
        sock.recvfrom.side_effect = [
            TimeoutError(),
            (Message.discovery_response().encode(), new),
            (Message.ack(2, 10, 42).encode(), new),
        ]
        with patch('client.discovery.print'):
            ack, address = send_request_and_wait_ack(sock, old, 2, 7, session='a' * 32)
        self.assertEqual(address, new)
        self.assertEqual(ack.total_sum, 42)
        sends = sock.sendto.call_args_list
        self.assertEqual(sends[0].args[1], old)
        self.assertEqual(Message.decode(sends[1].args[0]).type, 'DISCOVERY')
        self.assertEqual(sends[2].args[1], new)
        self.assertEqual(sends[0].args[0], sends[2].args[0])

    def test_input_reader_keeps_session_and_uses_new_leader(self):
        old = ('127.0.0.1', 4000)
        new = ('127.0.0.3', 4000)
        sock = Mock()
        output = queue.Queue()
        with patch('client.interface.sys.stdin', io.StringIO('bad\n-1\n7\n8\n')), patch(
            'client.interface.send_request_and_wait_ack',
            side_effect=[(Message.ack(1, 1, 7), new), (Message.ack(2, 2, 15), new)],
        ) as send:
            user_input_reader(sock, old, output, threading.Event())
        first, second = send.call_args_list
        self.assertEqual(first.args, (sock, old, 1, 7))
        self.assertEqual(second.args, (sock, new, 2, 8))
        self.assertEqual(first.kwargs['session'], second.kwargs['session'])
        self.assertEqual(len(first.kwargs['session']), 32)
        self.assertIn('server 127.0.0.3', output.get_nowait())
        packet, address = sock.sendto.call_args.args
        self.assertEqual(Message.decode(packet).type, 'EXIT')
        self.assertEqual(address, new)
