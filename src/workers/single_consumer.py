from threading import Event
from uuid import uuid4

from app.kafka.consumers.single_order import SingleMessageConsumer
from workers.base import WorkerBaseEntrypoint, run_worker


def main() -> None:
    worker = WorkerBaseEntrypoint(
        name=f"single-message-consumer-{uuid4()}",
        run_func=SingleMessageConsumer().run,
    )
    run_worker(worker=worker, stop_event=Event())


if __name__ == "__main__":
    main()
