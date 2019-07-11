#!/usr/bin/env python3
from confluent_kafka import Consumer
from ssl import get_default_verify_paths
import flow_messages_enriched_pb2 as api # this needs to be in the local path

import ipaddress
from collections import defaultdict

import datetime

class TrieNode:
    def __init__(self, version=6, root=False):
        self.lnode = None
        self.rnode = None

        self.depth = 0 # prefix len
        self.content = 0 # should be a network address of correct afi
        self.volume = 0.0 # bit/s this net has seen

        self.__root = root
        self.version = version

        if self.version == 4:
            self.maxlen = 32
        elif self.version == 6:
            self.maxlen = 128
        else:
            raise NotImplementedError

    def __getitem__(self, bit):
        if bit == 0:
            return self.lnode
        elif bit == 1:
            return self.rnode
        else:
            raise IndexError

    def __setitem__(self, bit, node):
        if bit == 0:
            self.lnode = node
        elif bit == 1:
            self.rnode = node
        else:
            raise IndexError

    def __getattribute__(self, name):
        if name in ["insert"]:
            if self.__root:
                return object.__getattribute__(self, name)
            else:
                raise AttributeError("Method '{}' is only valid on the root node.".format(name))
        else:
            return object.__getattribute__(self, name)

    def netaddr(self):
        return "{}/{}".format(ipaddress.ip_address(self.content), self.depth)

    def insert(self, addr, volume):
        current = self
        content = 0
        for i in range(self.maxlen+1):
            current.depth = i
            current.volume += volume*8/300
            current.content = content
            if i < self.maxlen:
                bit = (addr >> (self.maxlen-1-i)) & 1
                if not current[bit]:
                    current[bit] = TrieNode(version=self.version)
                current = current[bit]
                content = content | (bit<<(self.maxlen-1-i))

    def get_prefixes(self, shapelimit=100*1024**2):
        # end of recursion, a leaf
        if not self.lnode and not self.rnode:
            if self.volume > shapelimit:
                return set([self]) # leafs may return themselves
            else:
                return set() # or -- more likely -- an empty set

        # start of recursion, for both children, if applicable
        prefixes = set()
        if self.lnode:
            prefixes.update(self.lnode.get_prefixes(shapelimit))
        if self.rnode:
            prefixes.update(self.rnode.get_prefixes(shapelimit))

        # if the aggregation of both children reaches the limit, return self
        if not prefixes and self.volume > shapelimit:
            prefixes.add(self)

        return prefixes

    def __str__(self):
        content = self.netaddr()
        lnode = "✔" if self.lnode else "✗"
        rnode = "✔" if self.rnode else "✗"
        volume = int(self.volume/1024**2)
        return f"<TrieNode {content} ({volume}Mbps) [{lnode}, {rnode}]>"

    def __repr__(self):
        return str(self)

class BitCounter:
    def __init__(self):
        self.ipv4_trie = TrieNode(version=4, root=True)
        self.ipv6_trie = TrieNode(version=6, root=True)

    def insert(self, flowmsg):
        if flowmsg.IPversion == 4:
            return self.ipv4_trie.insert(int.from_bytes(flowmsg.DstIP, "big"), int(flowmsg.Bytes))
        elif flowmsg.IPversion == 6:
            return self.ipv6_trie.insert(int.from_bytes(flowmsg.DstIP, "big"), int(flowmsg.Bytes))
        else:
            raise NotImplementedError

    def get_total(self):
        return self.ipv4_trie.volume + self.ipv6_trie.volume

    def get_prefixes(self, shapelimit):
        return self.ipv4_trie.get_prefixes(shapelimit), self.ipv6_trie.get_prefixes(shapelimit)

if __name__ == "__main__":

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

    customers = {
            10101: 3000,
            10102: 375,
            10103: 100,
            10104: 100,
            10105: 100,
            10106: 100,
            10107: 100000,
            10108: 500,
            10109: 100,
            10209: 100
            }

    try:
        while True:
            counters = defaultdict(BitCounter)
            finish_time = datetime.datetime.now() + datetime.timedelta(minutes=5)
            while datetime.datetime.now() < finish_time:
                raw = consumer.poll()
                if raw.error():
                    continue

                flowmsg = api.FlowMessage()
                flowmsg.ParseFromString(raw.value())

                if "DFN" in flowmsg.Peer and flowmsg.Direction == 1 and flowmsg.Cid in customers.keys():
                    counters[flowmsg.Cid].insert(flowmsg)
            for cid, counter in counters.items():
                v4, v6 = counter.get_prefixes(customers[cid]/2*1024**2)
                for p in v4 | v6:
                    if int(counter.get_total()/1024**2) >= customers[cid]:
                        print("{}: {} - {} averaged {}Mbps (of {}Mbps total) over last 5min, allowed is {}Mbps".format(finish_time, cid, p.netaddr(), int(p.volume/1024**2), int(counter.get_total()/1024**2), customers[cid]))
    except KeyboardInterrupt:
        consumer.close()
