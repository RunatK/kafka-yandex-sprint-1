import signal
import unittest
from threading import Event
from unittest.mock import MagicMock, patch

from workers import batch_consumer, producer, single_consumer
from workers.base import WorkerBaseEntrypoint, run_worker


class WorkerTests(unittest.TestCase):
    @patch('workers.base.signal.signal')
    def test_worker_runs_once(self, register: MagicMock) -> None:
        stop = Event()
        run = MagicMock()
        run_worker(WorkerBaseEntrypoint('test', run), stop)
        run.assert_called_once_with(stop)
        self.assertFalse(stop.is_set())
        self.assertEqual(register.call_count, 2)

    @patch('workers.base.signal.signal')
    def test_failure_stops_worker_and_propagates(self, register: MagicMock) -> None:
        stop = Event()
        run = MagicMock(side_effect=RuntimeError('worker failure'))
        with self.assertRaisesRegex(RuntimeError, 'worker failure'):
            run_worker(WorkerBaseEntrypoint('test', run), stop)
        self.assertTrue(stop.is_set())
        run.assert_called_once_with(stop)

    def test_termination_signals_request_shutdown(self) -> None:
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signum=signum):
                stop = Event()

                def register(
                    number: int, handler: object, expected_signum: int = signum,
                ) -> None:
                    if number == expected_signum:
                        self.assertTrue(callable(handler))
                        if callable(handler):
                            handler(number, None)

                with patch('workers.base.signal.signal', side_effect=register):
                    run_worker(WorkerBaseEntrypoint('test', lambda event: None), stop)
                self.assertTrue(stop.is_set())

    def test_each_entrypoint_starts_only_its_worker(self) -> None:
        for module, attribute in (
            (producer, 'run_producer'),
            (single_consumer, 'SingleMessageConsumer'),
            (batch_consumer, 'BatchMessageConsumer'),
        ):
            with (
                self.subTest(module=module.__name__),
                patch.object(module, 'run_worker') as runner,
                patch.object(module, attribute) as target,
            ):
                module.main()
                runner.assert_called_once()
                worker = runner.call_args.kwargs['worker']
                expected = target if module is producer else target.return_value.run
                self.assertIs(worker.run_func, expected)
                self.assertIsInstance(runner.call_args.kwargs['stop_event'], Event)
                self.assertFalse(runner.call_args.kwargs['stop_event'].is_set())
