from __future__ import annotations

import struct
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


_MESSAGE_LAYOUTS: dict[
    MessageType, tuple[int, struct.Struct, tuple[str, ...]]
] = {
    MessageType.DISCOVERY: (1, struct.Struct("!B"), ()),
    MessageType.DISCOVERY_RESP: (2, struct.Struct("!B"), ()),
    MessageType.REQUEST: (3, struct.Struct("!BQQ"), ("id_req", "value")),
    MessageType.ACK: (
        4,
        struct.Struct("!BQQQ"),
        ("id_req", "num_reqs", "total_sum"),
    ),
    MessageType.EXIT: (5, struct.Struct("!B"), ()),
}
_TYPE_BY_TAG = {tag: msg_type for msg_type, (tag, _, _) in _MESSAGE_LAYOUTS.items()}


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
        """Serializes the message using fixed-width network byte order fields."""
        try:
            tag, layout, fields = _MESSAGE_LAYOUTS[self.type]
        except KeyError:
            raise ValueError(f"Unknown message type: {self.type}") from None

        if self.payload.keys() != set(fields):
            raise ValueError(
                f"Invalid fields for {self.type.value}: expected {fields}"
            )
        try:
            return layout.pack(tag, *(self.payload[field] for field in fields))
        except (struct.error, TypeError) as error:
            raise ValueError(f"Invalid {self.type.value} payload: {error}") from None

    @classmethod
    def decode(cls, raw_bytes: bytes) -> Message:
        """Decodes a fixed-width network byte order message."""
        if not raw_bytes:
            raise ValueError("Cannot decode empty data")

        try:
            (tag,) = struct.unpack("!B", raw_bytes[:1])
        except struct.error as error:
            raise ValueError(f"Invalid binary payload: {error}") from None

        try:
            msg_type = _TYPE_BY_TAG[tag]
        except KeyError:
            raise ValueError(f"Unknown message type tag: {tag}") from None

        _, layout, fields = _MESSAGE_LAYOUTS[msg_type]
        if len(raw_bytes) != layout.size:
            raise ValueError(
                f"Invalid {msg_type.value} message size: "
                f"expected {layout.size}, got {len(raw_bytes)}"
            )

        try:
            values = layout.unpack(raw_bytes)[1:]
        except struct.error as error:
            raise ValueError(f"Invalid binary payload: {error}") from None

        if msg_type == MessageType.DISCOVERY:
            return cls.discovery()
        elif msg_type == MessageType.DISCOVERY_RESP:
            return cls.discovery_response()
        elif msg_type == MessageType.EXIT:
            return cls.exit()
        elif msg_type == MessageType.REQUEST:
            return cls.request(**dict(zip(fields, values)))
        elif msg_type == MessageType.ACK:
            return cls.ack(**dict(zip(fields, values)))

        raise ValueError(f"Unhandled message type: {msg_type}")