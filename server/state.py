import threading
from dataclasses import asdict, dataclass


@dataclass
class ClientRecord:
    ip: str
    port: int = 0
    session: str | None = None
    last_req: int = 0
    last_num_reqs: int = 0
    last_total_sum: int = 0

    @property
    def address(self) -> str:
        if self.port > 0:
            return f"{self.ip}:{self.port}"
        return self.ip


class ServerState:
    def __init__(self) -> None:
        self.num_reqs: int = 0
        self.total_sum: int = 0
        self.clients: dict[str | tuple[str, int], ClientRecord] = {}
        self._lock = threading.Lock()

    def register_client(self, ip: str, port: int = 0, session: str | None = None) -> ClientRecord:
        key = session if session is not None else (ip, port)
        with self._lock:
            if key not in self.clients:
                new_client = ClientRecord(ip=ip, port=port, session=session)
                self.clients[key] = new_client
                return new_client
            return self.clients[key]

    def get_client(self, ip: str, port: int = 0, session: str | None = None) -> ClientRecord | None:
        key = session if session is not None else (ip, port)
        with self._lock:
            return self.clients.get(key)

    def get_clients(self) -> list[ClientRecord]:
        with self._lock:
            return list(self.clients.values())

    def process_request(
        self, address: str, id_req: int, value: int, port: int = 0,
        session: str | None = None
    ) -> tuple[str, int, int]:
        """Processes an incoming request under lock.

        Status possibilities:
        - 'NORMAL': Expected next id_req (id_req == last_req + 1), sum accumulated.
        - 'DUPLICATE': Duplicate request (id_req <= last_req), no sum, retransmit last ACK.
        - 'GAP': Request jumped ahead (id_req > last_req + 1), previous packet lost,
                 no sum, reply with last processed id_req according to specification.

        Returns:
            A tuple (status, last_num_reqs, last_total_sum).
        """
        ip = address
        with self._lock:
            key = session if session is not None else (ip, port)
            if key not in self.clients:
                client = ClientRecord(ip=ip, port=port, session=session)
                self.clients[key] = client
            else:
                client = self.clients[key]

            # 1. Duplicate detection: request ID already processed
            if id_req <= client.last_req:
                return "DUPLICATE", client.last_num_reqs, client.last_total_sum

            # 2. Gap detection (PDF Page 3): id_req > last_req + 1
            # An intermediate packet was lost, do not sum, respond with last acknowledged state
            if id_req > client.last_req + 1:
                return "GAP", client.last_num_reqs, client.last_total_sum

            # 3. Normal expected in-order request: apply sum and advance counters
            self.num_reqs += 1
            self.total_sum += value

            client.last_req = id_req
            client.last_num_reqs = self.num_reqs
            client.last_total_sum = self.total_sum

            return "NORMAL", self.num_reqs, self.total_sum

    def snapshot(self):
        """JSON-safe independent copy, including duplicate-detection records."""
        with self._lock:
            return [self.num_reqs, self.total_sum,
                    [asdict(client) for client in self.clients.values()]]

    def restore(self, snapshot):
        num_reqs, total_sum, records = snapshot
        clients = {}
        for fields in records:
            client = ClientRecord(**fields)
            key = client.session if client.session is not None else (client.ip, client.port)
            clients[key] = client
        with self._lock:
            self.num_reqs, self.total_sum, self.clients = num_reqs, total_sum, clients
