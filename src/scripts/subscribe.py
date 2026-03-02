# This script is intended to be modified, the goal is to have manual producer to kafka
# Feel free to edit values / whatever for your testing purposes
from confluent_kafka import Consumer, TopicPartition, KafkaError

from drgn.kafka import kafka_config
import uuid
import argparse

from typing import cast


# defined in topics_config.yaml. if changed, need to rebuild the image.
TOPIC = "trades.topic"


def commit(err: KafkaError, topics: list[TopicPartition]):
    if not err.fatal():
        return
    err_dict: dict = {"origin": "on_commit"}
    if err:
        err_dict["error"] = err.str()
    if topics:
        err_dict["topics"] = [topic.topic for topic in topics]
    print(f"ERROR - {err_dict}")


def consume_orders(consumer: Consumer):
    while True:
        msgs = consumer.consume()
        if len(msgs) > 0:
            for msg in msgs:
                if error := msg.error():
                    code = error.code()
                    if code == KafkaError._PARTITION_EOF:  # type: ignore
                        print("No more msgs")
                    else:
                        print(code)
                else:
                    print(msg.timestamp(), msg.offset(), msg.value())
                    consumer.commit(message=msg)
        else:
            print("no message")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", dest="topic")
    parser.add_argument("--offset", dest="offset")
    args = parser.parse_args()

    topic = args.topic or TOPIC
    offset = args.offset or 0
    print(f"INFO: topic {topic} offset {offset}")
    consumer = Consumer(
        cast(
            dict,
            kafka_config
            | {
                "group.id": str(uuid.uuid4()),
                "on_commit": commit,
                "enable.auto.commit": False,
                "enable.partition.eof": True,
                "auto.offset.reset": "earliest",
            },
        )
    )
    consumer.assign([TopicPartition(topic, 0, int(offset))])

    consume_orders(consumer)
