from threading import Event
from uuid import uuid4

from app.kafka.producers.order import run_producer
from workers.base import WorkerBaseEntrypoint, run_worker


def main() -> None:
    worker = WorkerBaseEntrypoint(
        name=f"order-producer-{uuid4()}",
        run_func=run_producer,
    )
    run_worker(worker=worker, stop_event=Event())


if __name__ == "__main__":
    main()
