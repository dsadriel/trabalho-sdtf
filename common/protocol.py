from __future__ import annotations

import json
from enum import StrEnum
from typing import Any

# Maximum 64-bit unsigned integer limit (specification requirement)
UINT64_MAX = (1 << 64) - 1


class MessageType(StrEnum):
    DISCOVERY = "DISCOVERY"
    DISCOVERY_RESP = "DISCOVERY_RESP"
    REQUEST = "REQUEST"
    ACK = "ACK"
    EXIT = "EXIT"


class Message:
    """Unified network message for the application protocol."""

    def __init__(self, msg_type: MessageType, **kwargs: Any) -> None:
        self.type = msg_type
        self.payload = kwargs

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Message):
            return False
        return self.type == other.type and self.payload == other.payload

    def __repr__(self) -> str:
        return f"Message({self.type.value}, {self.payload})"

    # Named constructors

    @classmethod
    def discovery(cls) -> Message:
        return cls(MessageType.DISCOVERY)

    @classmethod
    def discovery_response(cls) -> Message:
        return cls(MessageType.DISCOVERY_RESP)

    @classmethod
    def exit(cls) -> Message:
        return cls(MessageType.EXIT)

    @classmethod
    def request(cls, id_req: int, value: int) -> Message:
        if not isinstance(id_req, int) or not 1 <= id_req <= UINT64_MAX:
            raise ValueError(f"id_req {id_req} must be a positive 64-bit unsigned integer")
        if not isinstance(value, int) or not 1 <= value <= UINT64_MAX:
            raise ValueError(f"value {value} must be a positive integer (1 to 2^64-1)")
        return cls(MessageType.REQUEST, id_req=id_req, value=value)

    @classmethod
    def ack(cls, id_req: int, num_reqs: int, total_sum: int) -> Message:
        for name, val in [
            ("id_req", id_req),
            ("num_reqs", num_reqs),
            ("total_sum", total_sum),
        ]:
            if not isinstance(val, int) or not 0 <= val <= UINT64_MAX:
                raise ValueError(f"{name} {val} must be a 64-bit unsigned integer")
        return cls(
            MessageType.ACK, id_req=id_req, num_reqs=num_reqs, total_sum=total_sum
        )

    # Field access properties

    def _get_field(self, name: str) -> int:
        if name not in self.payload:
            raise AttributeError(
                f"Message of type '{self.type.value}' does not have field '{name}'"
            )
        return self.payload[name]

    @property
    def id_req(self) -> int:
        return self._get_field("id_req")

    @property
    def value(self) -> int:
        return self._get_field("value")

    @property
    def num_reqs(self) -> int:
        return self._get_field("num_reqs")

    @property
    def total_sum(self) -> int:
        return self._get_field("total_sum")

    # Serialization and Deserialization

    def encode(self) -> bytes:
        """Serializes the message to UTF-8 encoded JSON bytes."""
        data = {"type": self.type.value, **self.payload}
        return json.dumps(data).encode("utf-8")

    @classmethod
    def decode(cls, raw_bytes: bytes) -> Message:
        """Decodes UTF-8 encoded JSON bytes from the network into a Message instance."""
        if not raw_bytes:
            raise ValueError("Cannot decode empty data")

        try:
            data = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"Invalid JSON payload: {error}") from None

        if not isinstance(data, dict) or "type" not in data:
            raise ValueError("Missing 'type' field in payload")

        type_str = data.pop("type")
        try:
            msg_type = MessageType(type_str)
        except ValueError:
            raise ValueError(f"Unknown message type: {type_str}") from None

        if msg_type == MessageType.DISCOVERY:
            return cls.discovery()
        elif msg_type == MessageType.DISCOVERY_RESP:
            return cls.discovery_response()
        elif msg_type == MessageType.EXIT:
            return cls.exit()
        elif msg_type == MessageType.REQUEST:
            if "id_req" not in data or "value" not in data:
                raise ValueError("REQUEST message missing 'id_req' or 'value'")
            return cls.request(id_req=data["id_req"], value=data["value"])
        elif msg_type == MessageType.ACK:
            for field in ("id_req", "num_reqs", "total_sum"):
                if field not in data:
                    raise ValueError(f"ACK message missing '{field}'")
            return cls.ack(
                id_req=data["id_req"],
                num_reqs=data["num_reqs"],
                total_sum=data["total_sum"],
            )

        raise ValueError(f"Unhandled message type: {msg_type}")