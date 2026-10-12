import queue
import threading

from common.utils import format_timestamp


class ServerInterface:
    """Asynchronous server interface subservice using a dedicated console writer thread."""

    def __init__(self) -> None:
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _worker(self) -> None:
        while True:
            msg = self._queue.get()
            if msg is None:
                self._queue.task_done()
                break
            print(msg, flush=True)
            self._queue.task_done()

    def start(self) -> None:
        """Starts the dedicated background console writer thread."""
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._worker,
                    name="ServerScreenWriterThread",
                    daemon=True,
                )
                self._thread.start()

    def stop(self) -> None:
        """Flushes all queued console logs and stops the writer thread."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                self._queue.put(None)
                self._thread.join(timeout=2.0)
                self._thread = None

    def log(self, message: str) -> None:
        """Enqueues message for asynchronous output or prints synchronously if inactive."""
        if self._thread is not None and self._thread.is_alive():
            self._queue.put(message)
        else:
            print(message, flush=True)

    def print_startup(self, num_reqs: int = 0, total_sum: int = 0) -> None:
        """Prints the mandatory initial server startup message."""
        print(
            f"{format_timestamp()} num_reqs {num_reqs} total_sum {total_sum}",
            flush=True,
        )

    def print_request(
        self,
        client_display: str,
        id_req: int,
        value: int,
        num_reqs: int,
        total_sum: int,
    ) -> None:
        """Enqueues normal request log line in the exact specification format."""
        self.log(
            f"{format_timestamp()} client {client_display} "
            f"id_req {id_req} value {value} num_reqs {num_reqs} total_sum {total_sum}"
        )

    def print_duplicate(
        self,
        client_display: str,
        id_req: int,
        value: int,
        num_reqs: int,
        total_sum: int,
    ) -> None:
        """Enqueues duplicate request log line with DUP!! in the exact specification format."""
        self.log(
            f"{format_timestamp()} client {client_display} DUP!! "
            f"id_req {id_req} value {value} num_reqs {num_reqs} total_sum {total_sum}"
        )
