from common.utils import format_timestamp


def print_server_startup(num_reqs: int = 0, total_sum: int = 0) -> None:
    """Prints the mandatory initial server startup message."""
    print(
        f"{format_timestamp()} num_reqs {num_reqs} total_sum {total_sum}",
        flush=True,
    )


def print_server_request(
    client_display: str,
    id_req: int,
    value: int,
    num_reqs: int,
    total_sum: int,
) -> None:
    """Prints normal request processing log in the required specification format."""
    print(
        f"{format_timestamp()} client {client_display} "
        f"id_req {id_req} value {value} num_reqs {num_reqs} total_sum {total_sum}",
        flush=True,
    )


def print_server_duplicate(
    client_display: str,
    id_req: int,
    value: int,
    num_reqs: int,
    total_sum: int,
) -> None:
    """Prints duplicate request log with DUP!! marker in the required specification format."""
    print(
        f"{format_timestamp()} client {client_display} DUP!! "
        f"id_req {id_req} value {value} num_reqs {num_reqs} total_sum {total_sum}",
        flush=True,
    )
