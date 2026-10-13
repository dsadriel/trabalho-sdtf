import json
import unittest
from datetime import datetime

from common.protocol import (
    Message,
    MessageType,
)
from common.utils import format_timestamp, parse_port


class TestProtocolJSON(unittest.TestCase):
    def test_discovery_message_serialization(self):
        msg = Message.discovery()
        raw_bytes = msg.encode()
        self.assertIsInstance(raw_bytes, bytes)

        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.type, MessageType.DISCOVERY)
        self.assertEqual(decoded, msg)

    def test_discovery_resp_message_serialization(self):
        msg = Message.discovery_response()
        raw_bytes = msg.encode()
        self.assertIsInstance(raw_bytes, bytes)

        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.type, MessageType.DISCOVERY_RESP)
        self.assertEqual(decoded, msg)

    def test_request_message_serialization(self):
        msg = Message.request(id_req=1, value=42)
        raw_bytes = msg.encode()

        # Verify that payload is valid JSON
        parsed_json = json.loads(raw_bytes.decode("utf-8"))
        self.assertEqual(parsed_json["type"], "REQUEST")
        self.assertEqual(parsed_json["id_req"], 1)
        self.assertEqual(parsed_json["value"], 42)

        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.id_req, 1)
        self.assertEqual(decoded.value, 42)
        self.assertEqual(decoded, msg)

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

        decoded = Message.decode(raw_bytes)
        self.assertEqual(decoded.id_req, 5)
        self.assertEqual(decoded.num_reqs, 100)
        self.assertEqual(decoded.total_sum, uint64_max)
        self.assertEqual(decoded, msg)

    def test_ack_message_overflow_uint64_raises(self):
        with self.assertRaises(ValueError):
            Message.ack(id_req=1, num_reqs=1, total_sum=(1 << 64))

    def test_decode_empty_or_truncated(self):
        with self.assertRaises(ValueError):
            Message.decode(b"")

        with self.assertRaises(ValueError):
            Message.decode(b"not a json")

    def test_decode_missing_fields_or_unknown_type(self):
        with self.assertRaises(ValueError):
            Message.decode(b'{"type": "UNKNOWN"}')

        with self.assertRaises(ValueError):
            # REQUEST missing id_req or value
            Message.decode(b'{"type": "REQUEST", "id_req": 1}')

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
