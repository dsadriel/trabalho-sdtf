from datetime import datetime

def parse_port(value: str) -> int:
    """Converts a port string to an int.

    Raises ValueError with a readable message if the value is not a
    number or is outside the valid range.
    """
    from common.config import MAX_PORT, MIN_PORT
    
    try:
        port = int(value)
    except ValueError:
        raise ValueError(f"port '{value}' is not a number") from None

    if not MIN_PORT <= port <= MAX_PORT:
        raise ValueError(
            f"port {port} is out of range ({MIN_PORT}-{MAX_PORT})"
        )

    return port


def format_timestamp(time: datetime | None = None) -> str:
    """Returns the formatted date and time as 'YYYY-MM-DD HH:MM:SS'."""
    if time is None:
        time = datetime.now()
    return time.strftime("%Y-%m-%d %H:%M:%S")
