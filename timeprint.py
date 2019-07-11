#!/usr/bin/env python3
import confluent_kafka
import flow_messages_enriched_pb2 as api # this needs to be in the local path
import helpers as h # this needs to be in the local path

import sys
import ipaddress
from collections import defaultdict
from ssl import get_default_verify_paths
import time

with open("./authdata","r") as f:
    lines = f.readlines()
    group = lines[0].strip()
    username = lines[1].strip()
    password = lines[2].strip()

consumer = confluent_kafka.Consumer(
    {
        "bootstrap.servers": "kafka01.bwnf.belwue.de:9093,kafka02.bwnf.belwue.de:9093,kafka03.bwnf.belwue.de:9093,kafka04.bwnf.belwue.de:9093,kafka05.bwnf.belwue.de:9093",
        "group.id": group,
        "security.protocol": "sasl_ssl",
        "ssl.ca.location": get_default_verify_paths().cafile,
        "sasl.mechanisms": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
    }
)
def go_to_oldest(consumer, partitions):
    for p in partitions:
        p.offset = confluent_kafka.OFFSET_BEGINNING
    consumer.assign(partitions)
def go_to_newest(consumer, partitions):
    for p in partitions:
        p.offset = confluent_kafka.OFFSET_END
    consumer.assign(partitions)
consumer.subscribe(['flow-messages-enriched'], on_assign=go_to_newest)


try:
    lag = 0.0
    count = 0
    while True:
        raw = consumer.poll()
        if raw.error():
            continue
        else:
            if count == 0:
                flowmsg = api.FlowMessage()
                flowmsg.ParseFromString(raw.value())
                print("Received:", flowmsg.TimeReceived)
                print("FlowStart:", flowmsg.TimeFlowStart)
                print("FlowEnd:", flowmsg.TimeFlowEnd)
            count += 1
        flowmsg = api.FlowMessage()
        flowmsg.ParseFromString(raw.value())
        lag += int(time.time()) - flowmsg.TimeFlowEnd

        if count >= 30000:
            lag = lag/count
            count = 1
            sys.stdout.write('\x0d')
            sys.stdout.write("{:.2f}".format(lag))
            sys.stdout.flush()

except KeyboardInterrupt:
    print()
consumer.close()
