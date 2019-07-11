#!/usr/bin/env python3
from confluent_kafka import Consumer
import flow_messages_enriched_pb2 as api # this needs to be in the local path
from ssl import get_default_verify_paths

consumer = Consumer(
    {
        "bootstrap.servers": "kafka01.bwnf.belwue.de:9093,kafka02.bwnf.belwue.de:9093,kafka03.bwnf.belwue.de:9093,kafka04.bwnf.belwue.de:9093,kafka05.bwnf.belwue.de:9093",
        "group.id": "anon-test",
        "security.protocol": "sasl_ssl",
        "ssl.ca.location": get_default_verify_paths().cafile,
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "anon",
        "sasl.password": "anon",
    }
)
consumer.subscribe(['flow-messages-anon'])

try:
    while True:
        raw = consumer.poll()
        if raw.error():
            print(raw.error())
            continue

        flowmsg = api.FlowMessage()
        flowmsg.ParseFromString(raw.value())

        print(flowmsg)

except KeyboardInterrupt:
    pass
consumer.close()
