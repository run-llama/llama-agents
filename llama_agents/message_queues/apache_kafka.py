"""Apache Kafka Message Queue."""

import asyncio
import json
from logging import getLogger
from typing import Any, Callable, Coroutine, Dict, List, Optional
from llama_agents import CallableMessageConsumer, QueueMessage
from llama_agents.message_queues.base import (
    BaseMessageQueue,
)
from llama_agents.message_consumers.base import (
    BaseMessageQueueConsumer,
)

import logging

logger = getLogger(__name__)
logger.setLevel(logging.INFO)


DEFAULT_URL = "localhost:9092"
DEFAULT_TOPIC_PARTITIONS = 10
DEFAULT_TOPIC_REPLICATION_FACTOR = 1
DEFAULT_GROUP_ID = "default_group"  # single group for competing consumers


class KafkaMessageQueue(BaseMessageQueue):
    """Apache Kafka integration with aiokafka."""

    url: str = DEFAULT_URL

    def __init__(
        self,
        url: str = DEFAULT_URL,
    ) -> None:
        super().__init__(url=url)

    @classmethod
    def from_url_params(
        cls,
        host: str,
        port: Optional[int] = None,
    ) -> "KafkaMessageQueue":
        url = f"{host}:{port}" if port else f"{host}"
        return cls(url=url)

    def _create_new_topic(
        self,
        topic_name: str,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Create a new topic.

        Use kafka-python-ng instead of aio-kafka as latter has issues with
        resolving api_version with broker.

        TODO: convert to aiokafka once this it is resolved there.
        """
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin_client = KafkaAdminClient(bootstrap_servers=self.url)
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions or DEFAULT_TOPIC_PARTITIONS,
                replication_factor=replication_factor
                or DEFAULT_TOPIC_REPLICATION_FACTOR,
                **kwargs,
            )
            admin_client.create_topics(new_topics=[topic])
            logger.info(f"New topic {topic_name} created.")
        except TopicAlreadyExistsError:
            logger.info(f"Topic {topic_name} already exists.")
            pass

    async def _publish(self, message: QueueMessage) -> Any:
        """Publish message to the queue."""
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(bootstrap_servers=self.url)
        await producer.start()
        try:
            message_body = json.dumps(message.model_dump()).encode("utf-8")
            await producer.send_and_wait(message.type, message_body)
            logger.info(f"published message {message.id_}")
        finally:
            await producer.stop()

    async def cleanup_local(
        self, message_types: List[str], *args: Any, **kwargs: Dict[str, Any]
    ) -> None:
        """Cleanup for local runs.

        Use kafka-python-ng instead of aio-kafka as latter has issues with
        resolving api_version with broker when using admin client.

        TODO: convert to aiokafka once this it is resolved there.
        """
        from kafka.admin import KafkaAdminClient

        admin_client = KafkaAdminClient(bootstrap_servers=self.url)
        active_topics = admin_client.list_topics()
        topics_to_delete = [el for el in message_types if el in active_topics]
        admin_client.delete_consumer_groups(DEFAULT_GROUP_ID)
        if topics_to_delete:
            admin_client.delete_topics(topics_to_delete)

    async def deregister_consumer(self, consumer: BaseMessageQueueConsumer) -> Any:
        """Deregister a consumer."""
        pass

    async def launch_local(self) -> asyncio.Task:
        return asyncio.create_task(self.processing_loop())

    async def launch_server(self) -> None:
        """Launch server."""
        pass

    async def processing_loop(self) -> None:
        pass

    async def register_consumer(
        self, consumer: BaseMessageQueueConsumer
    ) -> Callable[..., Coroutine[Any, Any, None]]:
        """Register a new consumer."""
        from aiokafka import AIOKafkaConsumer

        # register topic
        self._create_new_topic(consumer.message_type)
        kafka_consumer = AIOKafkaConsumer(
            consumer.message_type,
            bootstrap_servers=self.url,
            group_id=DEFAULT_GROUP_ID,
            auto_offset_reset="earliest",
        )
        await kafka_consumer.start()

        logger.info(
            f"Registered consumer {consumer.id_}: {consumer.message_type}",
        )

        async def start_consuming_callable() -> None:
            """StartConsumingCallable."""
            try:
                async for msg in kafka_consumer:
                    decoded_message = json.loads(msg.value.decode("utf-8"))
                    queue_message = QueueMessage.model_validate(decoded_message)
                    await consumer.process_message(queue_message)
            finally:
                stop_task = asyncio.create_task(kafka_consumer.stop())
                stop_task.add_done_callback(
                    lambda _: logger.info(
                        f"stopped kafka consumer {consumer.id_}: {consumer.message_type}"
                    )
                )
                await asyncio.shield(stop_task)

        return start_consuming_callable


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.addHandler(logging.StreamHandler(stream=sys.stdout))

    parser = argparse.ArgumentParser()
    parser.add_argument("--produce", action="store_true", default=False)
    parser.add_argument("--consume", action="store_true", default=False)
    parser.add_argument("--clean-up", action="store_true", default=False)

    args = parser.parse_args()

    async def consume() -> None:
        mq = KafkaMessageQueue()

        # register a sample consumer
        def message_handler(message: QueueMessage) -> None:
            print(f"MESSAGE: {message}")

        test_consumer = CallableMessageConsumer(
            message_type="test", handler=message_handler
        )

        start_consuming_callable = await mq.register_consumer(test_consumer)
        await start_consuming_callable()

    async def produce() -> None:
        mq = KafkaMessageQueue()
        mq._create_new_topic(topic_name="test")

        test_message = QueueMessage(type="test", data={"message": "this is a test"})
        await mq.publish(test_message)

    async def clean_up() -> None:
        mq = KafkaMessageQueue()
        await mq.cleanup_local(["test"])

    if args.produce:
        asyncio.run(produce())

    if args.consume:
        asyncio.run(consume())

    if args.clean_up:
        asyncio.run(clean_up())
