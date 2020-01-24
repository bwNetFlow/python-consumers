#!/usr/bin/env python3
from confluent_kafka import Consumer, TopicPartition
import flow_messages_enriched_pb2 as api # this needs to be in the local path

import sys
import ipaddress
from collections import defaultdict
from ssl import get_default_verify_paths
import uuid
import sqlite3
from datetime import datetime


with open("./authdata","r") as f:
    lines = f.readlines()
    group = f"{lines[0].strip()}-{str(uuid.uuid1())}"
    username = lines[1].strip()
    password = lines[2].strip()

print(f"Running as Consumer Group {group}.")

consumer = Consumer(
    {
        "bootstrap.servers": "kafka01.bwnf.belwue.de:9093,kafka02.bwnf.belwue.de:9093,kafka03.bwnf.belwue.de:9093,kafka04.bwnf.belwue.de:9093,kafka05.bwnf.belwue.de:9093",
        "group.id": group,
        "security.protocol": "sasl_ssl",
        "ssl.ca.location": get_default_verify_paths().cafile,
        "sasl.mechanisms": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
        'auto.offset.reset': 'earliest',
    }
)
consumer.subscribe(['flow-messages-enriched'])

try:
    conn = sqlite3.connect('all_flows.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE flows
             (timeflowstart integer,
             srcaddr text, dstaddr text,
             srcport integer, dstport integer,
             proto integer, ethertype text,
             packets integer, bytes integer)''')

    count = 0
    records = []
    while True:
        count = count + 1
        raw = consumer.poll()
        if raw.error():
            continue

        flowmsg = api.FlowMessage()
        flowmsg.ParseFromString(raw.value())
        records.append((flowmsg.TimeFlowStart,
            str(ipaddress.ip_address(flowmsg.SrcAddr)), str(ipaddress.ip_address(flowmsg.DstAddr)),
            flowmsg.SrcPort, flowmsg.DstPort,
            flowmsg.Proto, flowmsg.Etype,
            flowmsg.Packets, flowmsg.Bytes)
        )

        if count > 100000:
            c.executemany("INSERT INTO flows VALUES (?,?,?,?,?,?,?,?,?)", records)
            count = 0
            records = []
            sys.stdout.write('\x0d')
            sys.stdout.write(f"Processed flows up to {datetime.fromtimestamp(flowmsg.TimeReceived).strftime('%H:%M:%S')}")
            sys.stdout.flush()

except KeyboardInterrupt:
    pass
conn.commit()
consumer.close()
