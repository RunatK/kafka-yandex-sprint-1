import logging
import signal
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import FrameType
from typing import NamedTuple

logger = logging.getLogger(__name__)


class WorkerBaseEntrypoint(NamedTuple):
    """
    Предоставляет все необходимое для запуска воркера
    """
    name: str
    run_func: Callable[[Event], None]


def run_worker(worker: WorkerBaseEntrypoint, stop_event: Event) -> None:
    def shutdown(signum: int, frame: FrameType | None) -> None:
        logger.info(f"Shutdown {worker.name} requested")
        stop_event.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    def run(worker: WorkerBaseEntrypoint) -> None:
        try:
            logger.info(f"Worker {worker.name} starting")
            worker.run_func(stop_event)
            logger.info(f"Worker {worker.name} stopped")
        except Exception:
            logger.exception(f"Worker {worker.name} failed")
            stop_event.set()
            raise

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run, worker)
        future.result()
