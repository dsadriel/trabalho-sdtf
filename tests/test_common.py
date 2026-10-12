import unittest
from datetime import datetime

from common.protocol import (
    Message,
    MessageType,
)
from common.utils import format_timestamp, parse_port


class TestProtocolBinary(unittest.TestCase):
    def test_discovery_message_serialization(self):
        msg = Message.discovery()
        raw_bytes = msg.encode()
        self.assertIsInstance(raw_bytes, bytes)
        self.assertEqual(raw_bytes, b"\x01")

        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.type, MessageType.DISCOVERY)
        self.assertEqual(decoded, msg)

    def test_discovery_resp_message_serialization(self):
        msg = Message.discovery_response()
        raw_bytes = msg.encode()
        self.assertIsInstance(raw_bytes, bytes)
        self.assertEqual(raw_bytes, b"\x02")

        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.type, MessageType.DISCOVERY_RESP)
        self.assertEqual(decoded, msg)

    def test_exit_message_serialization(self):
        msg = Message.exit()
        self.assertEqual(msg.encode(), b"\x05")
        self.assertEqual(Message.decode(msg.encode()), msg)

    def test_request_message_serialization(self):
        msg = Message.request(id_req=1, value=42)
        raw_bytes = msg.encode()

        self.assertEqual(len(raw_bytes), 17)
        self.assertEqual(
            raw_bytes, b"\x03" + (1).to_bytes(8, "big") + (42).to_bytes(8, "big")
        )

        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.id_req, 1)
        self.assertEqual(decoded.value, 42)
        self.assertEqual(decoded, msg)

    def test_request_message_serialization_with_uint64_max(self):
        uint64_max = (1 << 64) - 1
        msg = Message.request(id_req=uint64_max, value=uint64_max)
        raw_bytes = msg.encode()

        self.assertEqual(len(raw_bytes), 17)
        self.assertEqual(raw_bytes, b"\x03" + b"\xff" * 16)
        self.assertEqual(Message.decode(raw_bytes), msg)

    def test_request_message_non_positive_value_raises(self):
        with self.assertRaises(ValueError):
            Message.request(id_req=2, value=-15)
        with self.assertRaises(ValueError):
            Message.request(id_req=2, value=0)

    def test_ack_message_serialization_with_uint64(self):
        # Maximum value for unsigned 64-bit int (2**64 - 1)
        uint64_max = (1 << 64) - 1
        msg = Message.ack(id_req=5, num_reqs=100, total_sum=uint64_max)
        raw_bytes = msg.encode()

        self.assertEqual(len(raw_bytes), 25)
        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.id_req, 5)
        self.assertEqual(decoded.num_reqs, 100)
        self.assertEqual(decoded.total_sum, uint64_max)
        self.assertEqual(decoded, msg)

    def test_ack_message_big_endian_encoding(self):
        msg = Message.ack(id_req=1, num_reqs=2, total_sum=3)
        self.assertEqual(
            msg.encode(),
            b"\x04"
            + (1).to_bytes(8, "big")
            + (2).to_bytes(8, "big")
            + (3).to_bytes(8, "big"),
        )

    def test_ack_message_overflow_uint64_raises(self):
        with self.assertRaises(ValueError):
            Message.ack(id_req=1, num_reqs=1, total_sum=(1 << 64))

    def test_decode_empty_or_invalid_size(self):
        with self.assertRaises(ValueError):
            Message.decode(b"")

        with self.assertRaises(ValueError):
            Message.decode(b"\x03")

        with self.assertRaises(ValueError):
            Message.decode(b"\x01\x00")

    def test_decode_unknown_type_tags(self):
        for raw_bytes in (b"\x06", b"\xff"):
            with self.subTest(raw_bytes=raw_bytes):
                with self.assertRaisesRegex(ValueError, "Unknown message type tag"):
                    Message.decode(raw_bytes)

    def test_attribute_error_on_incompatible_fields(self):
        disc = Message.discovery()
        with self.assertRaises(AttributeError) as ctx:
            _ = disc.id_req
        self.assertIn("Message of type 'DISCOVERY' does not have field 'id_req'", str(ctx.exception))

        req = Message.request(id_req=1, value=10)
        with self.assertRaises(AttributeError):
            _ = req.total_sum


class TestUtils(unittest.TestCase):

    def test_format_timestamp(self):
        ts = format_timestamp()
        self.assertEqual(len(ts), 19)
        parsed = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        self.assertIsInstance(parsed, datetime)

    def test_format_timestamp_custom_time(self):
        custom_dt = datetime(2026, 1, 15, 10, 30, 0)
        self.assertEqual(format_timestamp(custom_dt), "2026-01-15 10:30:00")

    def test_parse_port_valid(self):
        self.assertEqual(parse_port("1"), 1)
        self.assertEqual(parse_port("4000"), 4000)
        self.assertEqual(parse_port("65535"), 65535)

    def test_parse_port_invalid(self):
        with self.assertRaises(ValueError):
            parse_port("0")
        with self.assertRaises(ValueError):
            parse_port("65536")
        with self.assertRaises(ValueError):
            parse_port("abc")


if __name__ == "__main__":
    unittest.main()
