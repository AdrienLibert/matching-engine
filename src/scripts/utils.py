from drgn.kafka import kafka_config
from confluent_kafka.admin import AdminClient

from typing import cast

if __name__ == "__main__":
    admin_client = AdminClient(cast(dict, kafka_config))

    print(admin_client.list_topics().topics)
