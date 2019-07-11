#!/usr/bin/env python3
import ipaddress
import datetime

from confluent_kafka import Consumer
from ssl import get_default_verify_paths

# The following needs to be available in PYTHONPATH
# https://github.com/bwNetFlow/protobuf
import flow_messages_enriched_pb2 as api

# example output:
"""
2019-09-10 15:04:07.913857: 192.168.24.87/32 averaged 815Mbps (of 6024Mbps total)
"""

# skip to line 129 for actual operation, the following classes are implementing the counting
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
            # safeguards insertions on non-root nodes
            if self.__root:
                return object.__getattribute__(self, name)
            else:
                raise AttributeError("Method '{}' is only valid on the root node.".format(name))
        else:
            # all other methods behave normally
            return object.__getattribute__(self, name)

    def netaddr(self):
        return "{}/{}".format(ipaddress.ip_address(self.content), self.depth)

    def insert(self, addr, volume):
        current = self
        content = 0
        for i in range(self.maxlen+1): # descend the trie, bit by bit
            current.depth = i
            current.volume += volume
            current.content = content
            if i < self.maxlen:
                bit = (addr >> (self.maxlen-1-i)) & 1
                if not current[bit]:
                    current[bit] = TrieNode(version=self.version)
                current = current[bit]
                content = content | (bit<<(self.maxlen-1-i))

    def get_prefixes(self, limit):
        # end of recursion, a leaf
        if not self.lnode and not self.rnode:
            if self.volume > limit:
                return set([self]) # leafs may return themselves
            else:
                return set() # or -- more likely -- an empty set

        # start of recursion, for both children, if applicable
        prefixes = set()
        if self.lnode:
            prefixes.update(self.lnode.get_prefixes(limit))
        if self.rnode:
            prefixes.update(self.rnode.get_prefixes(limit))

        # if the aggregation of both children reaches the limit, return self
        if not prefixes and self.volume > limit:
            prefixes.add(self)

        return prefixes

    def __str__(self):
        """ prettyprint self """
        content = self.netaddr()
        lnode = "✔" if self.lnode else "✗"
        rnode = "✔" if self.rnode else "✗"
        volume = int(self.volume)
        return f"<TrieNode {content} ({volume}Mbps) [{lnode}, {rnode}]>"

    def __repr__(self):
        return str(self)

class BitCounter:
    """ A class for counting bits by IP prefix and managing the tries. """
    def __init__(self):
        self.ipv4_trie = TrieNode(version=4, root=True)
        self.ipv6_trie = TrieNode(version=6, root=True)

    def insert(self, flowmsg):
        if flowmsg.Etype == 0x0800: # ipv4
            self.ipv4_trie.insert(int.from_bytes(flowmsg.DstAddr, "big"), int(flowmsg.Bytes))
        elif flowmsg.Etype == 0x86dd: # ipv6
            self.ipv6_trie.insert(int.from_bytes(flowmsg.DstAddr, "big"), int(flowmsg.Bytes))
        else:
            raise NotImplementedError

    def get_total(self):
        return self.ipv4_trie.volume + self.ipv6_trie.volume

    def get_prefixes(self, limit):
        return self.ipv4_trie.get_prefixes(limit), self.ipv6_trie.get_prefixes(limit)

if __name__ == "__main__":
    # usernames are prefixed with the customer id
    USERNAME = "001-johndoe"
    PASSWORD = "ASECUREPASSWORDCONAiNSNUMBERS,2"
    # consumer group names are prefixed with the username
    GROUP = USERNAME+"-subnet_volume-dev"
    # boilerplate for initializing a Kafka consumer
    consumer = Consumer(
        {
            "bootstrap.servers": "kafka01.example.com:9093",
            "group.id": GROUP,
            "security.protocol": "sasl_ssl",
            "ssl.ca.location": get_default_verify_paths().cafile,
            "sasl.mechanisms": "PLAIN",
            "sasl.username": USERNAME,
            "sasl.password": PASSWORD,
        }
    )
    # the name of the topic is usually the customer id plus some description
    # in this case, it's the default customer-specific stream
    consumer.subscribe(['001-flows'])

    # the following limit is the threshold from which subnets will be logged
    # this obviously depends on the interval configured in line 163
    # alert limit:       xxx     MB
    VOLUME_ALERT_LIMIT = 100 * 1024**2

    try:
        while True:
            counter = BitCounter() # initialize a new counter
            # the following line determines the interval in which bytes are counted
            finish_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
            while datetime.datetime.now() < finish_time:
                raw = consumer.poll() # this fetches a flow message from Kafka
                if raw.error():
                    print(raw.error())
                    continue # errors are ignored in this example

                # parse the received message using protobuf
                flowmsg = api.FlowMessage()
                flowmsg.ParseFromString(raw.value())

                # the following filters for flows which are of interest:
                #     the flow is incoming    AND    from the peer in question
                if flowmsg.FlowDirection == 0 and "DFN" in flowmsg.SrcIfDesc:
                    counter.insert(flowmsg) # this function inserts the flow volume in a prefix trie
            v4, v6 = counter.get_prefixes(VOLUME_ALERT_LIMIT) # this function receives all highest nodes exceeding the limit...
            for p in v4 | v6: # ... and we print them all for now
                print(f"{finish_time}: {p.netaddr()} averaged {int(p.volume/1024**2)}Mbps (of {int(counter.get_total()/1024**2)}Mbps total)")
    except KeyboardInterrupt:
        consumer.close() # exit gracefully
