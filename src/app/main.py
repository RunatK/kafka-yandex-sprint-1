import logging
import signal
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import FrameType

from app.kafka.consumers.batch_order import BatchMessageConsumer
from app.kafka.consumers.single_order import SingleMessageConsumer
from app.kafka.producers.order import run_producer

logger = logging.getLogger(__name__)


def main() -> None:
    stop = Event()

    def shutdown(signum: int, frame: FrameType | None) -> None:
        logger.info("Shutdown requested")
        stop.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    def run(worker: Callable[[Event], None]) -> None:
        try:
            worker(stop)
        except Exception:
            logger.exception("Worker failed")
            stop.set()
            raise

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(run, worker) for worker in (
            run_producer, SingleMessageConsumer().run, BatchMessageConsumer().run,
        )]
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
