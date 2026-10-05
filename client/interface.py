import queue
import socket
import sys
import threading

from client.processing import send_request_and_wait_ack
from common.protocol import Message
from common.utils import format_timestamp


def screen_writer(
    output_queue: queue.Queue[str | None],
) -> None:
    """Thread dedicated to writing formatted messages to the screen.

    Consumes messages from the queue and prints them until the None sentinel
    is received, ensuring no messages are dropped on client termination.
    """
    while True:
        message = output_queue.get()
        if message is None:
            output_queue.task_done()
            break

        print(message, flush=True)
        output_queue.task_done()


def user_input_reader(
    sock: socket.socket,
    server_addr: tuple[str, int],
    output_queue: queue.Queue[str | None],
    stop_event: threading.Event,
) -> None:
    """Reads numbers from stdin, coordinates requests and queues output."""
    server_ip = server_addr[0]
    id_req = 1

    try:
        for line in sys.stdin:
            if stop_event.is_set():
                break

            line = line.strip()
            if not line:
                continue

            try:
                value = int(line)
                if value <= 0:
                    continue
            except ValueError:
                # Ignore invalid non-integer inputs
                continue

            ack = send_request_and_wait_ack(sock, server_addr, id_req, value)

            # Format required by specification:
            # <data> server <ip> id_req <n> value <v> num_reqs <n> total_sum <s>
            log_line = (
                f"{format_timestamp()} server {server_ip} "
                f"id_req {id_req} value {value} num_reqs {ack.num_reqs} total_sum {ack.total_sum}"
            )
            output_queue.put(log_line)

            id_req += 1

    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        stop_event.set()


def run_client_interface(sock: socket.socket, server_addr: tuple[str, int]) -> None:
    """Orchestrates separate threads for reading user input and writing to the screen.

    Meets the specification requirement:
    'A interface do cliente deve ter uma thread para escrever as mensagens na tela,
    e outra thread para ler os comandos digitados pelo(a) usuário(a).'
    """
    output_queue: queue.Queue[str | None] = queue.Queue()
    stop_event = threading.Event()

    # Thread 1: Writer thread dedicated to console output
    writer = threading.Thread(
        target=screen_writer,
        args=(output_queue,),
        name="ScreenWriterThread",
        daemon=True,
    )
    writer.start()

    try:
        # Thread 2: Main thread acts as user input reader
        user_input_reader(sock, server_addr, output_queue, stop_event)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        # Send EXIT message to server before terminating
        try:
            exit_msg = Message.exit().encode()
            sock.sendto(exit_msg, server_addr)
        except OSError:
            pass

        output_queue.put(None)  # Sentinel to unblock writer thread
        writer.join(timeout=1.0)
