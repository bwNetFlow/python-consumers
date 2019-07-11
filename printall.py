#!/usr/bin/env python3
from confluent_kafka import Consumer
import flow_messages_enriched_pb2 as api # this needs to be in the local path

import sys
import ipaddress
from collections import defaultdict
from ssl import get_default_verify_paths

with open("./authdata","r") as f:
    lines = f.readlines()
    group = lines[0].strip()
    username = lines[1].strip()
    password = lines[2].strip()

consumer = Consumer(
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
consumer.subscribe(['flow-messages-enriched'])

try:
    while True:
        raw = consumer.poll()
        if raw.error():
            continue

        flowmsg = api.FlowMessage()
        flowmsg.ParseFromString(raw.value())

        # TODO: beliebige Filter hier, z.B.
        # if flowmsg.Peer == "Cogent":
        #     print(ipaddress.ip_address(flowmsg.DstIP))

        # oder einfach alle printen:
        print(flowmsg)

except KeyboardInterrupt:
    pass
consumer.close()
