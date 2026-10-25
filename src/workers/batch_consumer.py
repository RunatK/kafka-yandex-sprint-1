from threading import Event
from uuid import uuid4

from app.kafka.consumers.batch_order import BatchMessageConsumer
from workers.base import WorkerBaseEntrypoint, run_worker


def main() -> None:
    worker = WorkerBaseEntrypoint(
        name=f"batch-message-consumer-{uuid4()}",
        run_func=BatchMessageConsumer().run,
    )
    run_worker(worker=worker, stop_event=Event())


if __name__ == "__main__":
    main()
