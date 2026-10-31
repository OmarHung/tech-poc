"""One line per event, with a timestamp, on stdout (the verifier collects it into verify/logs)."""

from __future__ import annotations

import logging
import sys

FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
DATE_FORMAT = "%H:%M:%S"


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=FORMAT, datefmt=DATE_FORMAT)
    # uvicorn set up its own handlers before calling the app factory; give its lines the same timestamps.
    formatter = logging.Formatter(FORMAT, DATE_FORMAT)
    for name in ("uvicorn", "uvicorn.access"):
        for handler in logging.getLogger(name).handlers:
            handler.setFormatter(formatter)
    # httpx logs every request at INFO; at a few hundred requests a second that buries everything else.
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
