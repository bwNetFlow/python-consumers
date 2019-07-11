#!/usr/bin/env python3
from confluent_kafka import Consumer
import flow_messages_enriched_pb2 as api # this needs to be in the local path

import sys
from datetime import datetime
import socket
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

proto_table = {num:name[8:] for name,num in vars(socket).items() if name.startswith("IPPROTO")}
fwdir_table = {
        0 : 'unknown',
        1 : 'incoming',
        2 : 'outgoing'
        }
fwstatus_table = {
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


        target_cid = 19064
        if flowmsg.Cid == target_cid:
            router = ipaddress.ip_address(flowmsg.RouterAddr)
            peer = flowmsg.Peer
            time = datetime.utcfromtimestamp(flowmsg.TimeFlow).strftime('%Y-%m-%d %H:%M:%S')
            src = ipaddress.ip_address(flowmsg.SrcIP)
            dst = ipaddress.ip_address(flowmsg.DstIP)
            nexthop = ipaddress.ip_address(flowmsg.NextHop)
            src_port = flowmsg.SrcPort
            dst_port = flowmsg.DstPort
            proto = proto_table.get(flowmsg.Proto, "other")
            kbytes = flowmsg.Bytes/1024
            packets = flowmsg.Packets
            direction = fwdir_table[flowmsg.Direction]
            if direction == "incoming":
                backbone = "to backbone router " + flowmsg.DstIfDesc
            elif direction == "outgoing":
                backbone = "from backbone router " + flowmsg.SrcIfDesc
            else:
                continue
            print("-----------------------------------------------------------")
            print(f"{time}: {src}:{src_port} --({proto})--> {dst}:{dst_port}")
            print(f"    {direction} via {peer} on {router}, {backbone}")
            print(f"    total volume of {kbytes}kB in {packets} packets")
except KeyboardInterrupt:
    pass
consumer.close()
