import json
import unittest
from threading import Event
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError, validate

from app.infra.entities.order import Order
from app.kafka.consumers.base import OrderConsumer
from app.kafka.consumers.batch_order import BatchMessageConsumer
from app.kafka.consumers.single_order import SingleMessageConsumer
from app.kafka.producers.base import get_producer
from app.kafka.serialization import ORDER_SCHEMA


def message(offset: int) -> MagicMock:
    result = MagicMock()
    result.error.return_value = None
    result.offset.return_value = offset
    return result


class ConsumerTests(unittest.TestCase):
    def mock_processing(self, worker: OrderConsumer) -> MagicMock:
        process = MagicMock()
        patcher = patch.object(worker, "process", process)
        patcher.start()
        self.addCleanup(patcher.stop)
        return process

    def test_short_fetches_accumulate_before_one_commit(self) -> None:
        stop = Event()
        consumer = MagicMock()
        records = [message(i) for i in range(10)]
        consumer.consume.side_effect = [records[:4], records[4:]]
        consumer.commit.side_effect = lambda **kwargs: stop.set()
        worker = BatchMessageConsumer()
        process = self.mock_processing(worker)
        worker.consume(consumer, MagicMock(), stop)
        self.assertEqual(process.call_count, 10)
        self.assertEqual(consumer.store_offsets.call_count, 10)
        consumer.commit.assert_called_once_with(asynchronous=False)
        self.assertEqual(consumer.consume.call_args_list[1].kwargs['num_messages'], 6)

    def test_commit_failure_retries_existing_batch(self) -> None:
        stop = MagicMock()
        stop.is_set.side_effect = [False, False, True]
        consumer = MagicMock()
        consumer.consume.return_value = [message(i) for i in range(10)]
        consumer.commit.side_effect = [RuntimeError('commit unavailable'), None]
        worker = BatchMessageConsumer()
        process = self.mock_processing(worker)
        worker.consume(consumer, MagicMock(), stop)
        self.assertEqual(consumer.commit.call_count, 2)
        consumer.consume.assert_called_once()
        self.assertEqual(process.call_count, 20)

    def test_partial_batch_is_not_committed_on_shutdown(self) -> None:
        stop = Event()
        consumer = MagicMock()

        def fetch(**kwargs: float) -> list[MagicMock]:
            stop.set()
            return [message(0)]

        consumer.consume.side_effect = fetch
        worker = BatchMessageConsumer()
        process = self.mock_processing(worker)
        worker.consume(consumer, MagicMock(), stop)
        process.assert_not_called()
        consumer.commit.assert_not_called()

    def test_rebalance_discards_records_from_previous_owner(self) -> None:
        stop = Event()
        consumer = MagicMock()
        worker = BatchMessageConsumer()
        process = self.mock_processing(worker)
        records = [message(i) for i in range(10)]
        calls = 0

        def fetch(**kwargs: float) -> list[MagicMock]:
            nonlocal calls
            calls += 1
            if calls == 1:
                return records[:4]
            if calls == 2:
                consumer.subscribe.call_args.kwargs['on_revoke'](consumer, [])
                return records[4:]
            stop.set()
            return []

        consumer.consume.side_effect = fetch
        worker.consume(consumer, MagicMock(), stop)
        process.assert_not_called()
        consumer.commit.assert_not_called()

    def test_single_logs_bad_payload_and_continues(self) -> None:
        stop = Event()
        consumer = MagicMock()
        records = [message(0), message(1)]
        consumer.poll.side_effect = records
        stored: list[MagicMock] = []

        def store(**kwargs: MagicMock) -> None:
            stored.append(kwargs['message'])
            if len(stored) == 2:
                stop.set()

        consumer.store_offsets.side_effect = store
        deserializer = MagicMock(side_effect=[ValueError('invalid payload'), Order('1', '2', [], 'waiting')])
        SingleMessageConsumer().consume(consumer, deserializer, stop)
        self.assertEqual(stored, records)
        consumer.commit.assert_not_called()

    def test_groups_and_offset_modes(self) -> None:
        single = SingleMessageConsumer().get_config()
        batch = BatchMessageConsumer().get_config()
        self.assertNotEqual(single['group.id'], batch['group.id'])
        self.assertTrue(single['enable.auto.commit'])
        self.assertFalse(batch['enable.auto.commit'])
        self.assertFalse(single['enable.auto.offset.store'])
        self.assertFalse(batch['enable.auto.offset.store'])


class SerializationTests(unittest.TestCase):
    def test_order_matches_schema(self) -> None:
        validate(Order('1', '2', ['coffee'], 'waiting')._asdict(), json.loads(ORDER_SCHEMA))

    def test_invalid_status_is_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            validate(Order('1', '2', [], 'unknown')._asdict(), json.loads(ORDER_SCHEMA))

    @patch('app.kafka.producers.base.Producer')
    def test_producer_flushes_when_body_fails(self, factory: MagicMock) -> None:
        factory.return_value.flush.return_value = 0
        with self.assertRaises(ValueError), get_producer():
            raise ValueError('failure')
        factory.return_value.flush.assert_called_once_with(130)


if __name__ == '__main__':
    unittest.main()
