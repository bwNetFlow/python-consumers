#!/usr/bin/env python3
from confluent_kafka import Consumer
import flow_messages_enriched_pb2 as api # this needs to be in the local path
import ipaddress

import sys
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

# formatting helpers
def make_string(c):
    string = str({k: "{0:.5%}".format(v/sum(c.values())) for k, v in c.items()})
    return string, len(string) + 1

fw_dir = {
        0 : 'unknown',
        1 : 'incoming',
        2 : 'outgoing'
        }
fw_status = {
        0 : 'Unknown',
        64 : 'Forwarded (Unknown)',
        65 : 'Forwarded (Fragmented)',
        66 : 'Forwarded (Not Fragmented)',
        128 : 'Dropped (Unknown)',
        129 : 'Dropped (ACL Deny)',
        130 : 'Dropped (ACL Drop)',
        131 : 'Dropped (Unroutable)',
        132 : 'Dropped (Adjacency)',
        133 : 'Dropped (Fragmented and DF set)',
        134 : 'Dropped (Bad Header Checksum)',
        135 : 'Dropped (Bad Total Length)',
        136 : 'Dropped (Bad Header Length)',
        137 : 'Dropped (Bad TTL)',
        138 : 'Dropped (Policer)',
        139 : 'Dropped (WRED)',
        140 : 'Dropped (RPF)',
        141 : 'Dropped (For Us)',
        142 : 'Dropped (Bad Output Interface)',
        143 : 'Dropped (Hardware)',
        192 : 'Consumed (Unknown)',
        193 : 'Consumed (Terminate Punt Adjacency)',
        194 : 'Consumed (Terminate Incomplete Adjacency)',
        195 : 'Consumed (Terminate For Us)'
        }

try:
    while True:
        raw = consumer.poll()
        if raw.error():
            continue

        flowmsg = api.FlowMessage()
        flowmsg.ParseFromString(raw.value())

        if flowmsg.ForwardingStatus < 192:
        # if flowmsg.ForwardingStatus < 128:
            r = ipaddress.ip_address(flowmsg.RouterAddr)
            src = ipaddress.ip_address(flowmsg.SrcIP)
            dst = ipaddress.ip_address(flowmsg.DstIP)
            if not src.is_global or not dst.is_global:
                if not flowmsg.DstIfName:
                    dstif = "none"
                else:
                    dstif = "{}, {}".format(flowmsg.DstIfName, flowmsg.DstIfDesc)
                if not flowmsg.Peer:
                    peer = "none"
                else:
                    peer = flowmsg.Peer

                print("{} ({}): {} ({}, {}) -> {} ({}) ({} by {})".format(fw_dir[flowmsg.Direction], peer, src, flowmsg.SrcIfName, flowmsg.SrcIfDesc, dst, dstif, fw_status.get(flowmsg.ForwardingStatus, "UNDOCUMENTED: {:08b}".format(flowmsg.ForwardingStatus)), r))
except KeyboardInterrupt:
    pass
consumer.close()
