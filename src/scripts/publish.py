# This script is intended to be modified, the goal is to have manual producer to kafka
# Feel free to edit values / whatever for your testing purposes
from confluent_kafka import Producer
from datetime import datetime, timezone
from functools import partial

from drgn.kafka import kafka_config

import uuid
import json
import time
import random
import argparse

from typing import cast


def delivery(err, msg):
    print(f"INFO: {msg.value()}")


def produce_order(producer: Producer, min_quantity: int = -50, max_quantity: int = 50):
    qty = random.randint(int(min_quantity), int(max_quantity))
    msg = {
        "order_id": str(uuid.uuid4()),
        "order_type": "limit",
        "price": random.randint(45, 60),
        "quantity": qty if qty != 0 else 1,
        "timestamp": int(
            datetime.now(timezone.utc).timestamp() * 1000000
        ),  # nanosecond
    }

    producer.produce(
        "orders.topic", bytes(json.dumps(msg), "utf-8"), on_delivery=delivery
    )
    producer.poll()


def produce_trade(producer: Producer):
    trade_id = str(uuid.uuid4())
    qty = random.randint(10, 50)
    price = random.uniform(40.0, 45.0)
    now = int(datetime.now(timezone.utc).timestamp() * 1000000)
    trade_left = {
        "trade_id": trade_id,
        "order_id": str(uuid.uuid4()),
        "quantity": qty,
        "price": price,
        "action": "BUY",
        "status": "PARTIAL",
        "timestamp": now,
    }
    trade_right = {
        "trade_id": trade_id,
        "order_id": str(uuid.uuid4()),
        "quantity": qty,
        "price": price,
        "action": "SELL",
        "status": "PARTIAL",
        "timestamp": now,
    }
    producer.produce(
        "trades.topic", bytes(json.dumps(trade_left), "utf-8"), on_delivery=delivery
    )
    producer.poll()
    producer.produce(
        "trades.topic", bytes(json.dumps(trade_right), "utf-8"), on_delivery=delivery
    )
    producer.poll()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", dest="model", default="order")
    parser.add_argument(
        "--wait-for-input", dest="wait_for_input", default=False, action="store_true"
    )
    parser.add_argument("--min-qty", dest="min_quantity", default=-50)
    parser.add_argument("--max-qty", dest="max_quantity", default=50)
    args = parser.parse_args()

    producer = Producer(cast(dict, kafka_config))

    func = (
        partial(produce_order, producer, args.min_quantity, args.max_quantity)
        if args.model == "order"
        else partial(produce_trade, producer)
    )

    count = 0
    while True:
        func()
        if args.wait_for_input:
            _ = input()
        else:
            time.sleep(1.0)
