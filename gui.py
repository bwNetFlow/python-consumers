#!/usr/bin/env python3
import sys
import datetime
import curses
from curses import wrapper
from collections import defaultdict

import confluent_kafka
from confluent_kafka import Consumer
from ssl import get_default_verify_paths
import flow_messages_enriched_pb2 as api # this needs to be in the local path

with open("./authdata","r") as f:
    lines = f.readlines()
    group = lines[0].strip()
    username = lines[1].strip()
    password = lines[2].strip()

def go_to_newest(consumer, partitions):
    for p in partitions:
        p.offset = confluent_kafka.OFFSET_END
    consumer.assign(partitions)

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
consumer.subscribe(['flow-messages-enriched'], on_assign=go_to_newest)


class Screen():
    # graphics {{{
    def __init__(self):
        self.stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)
        self.stdscr.keypad(1)
        try:
            curses.start_color()
        except:
            pass
        self.stdscr.clear()

        self.w_geo = curses.newwin(6+2, 40, 0, 40)
        self.w_afi = curses.newwin(3+2, 40, 5, 0)
        self.w_avgs = curses.newwin(3+2, 40, 0, 0)
        self.w_dir = curses.newwin(3+2, 40, 10, 0)
        self.w_proto = curses.newwin(5+2, 40, 8, 40)
        self.w_as = curses.newwin(6+2, 40, 15, 0)
        self.w_peer = curses.newwin(6+2, 40, 15, 40)
        self.winlist = [self.w_geo, self.w_proto, self.w_afi, self.w_avgs, self.w_dir, self.w_as, self.w_peer]

        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_CYAN)
        self.w_avgs.addstr(1, 1, " "*38, curses.color_pair(1))
        self.w_avgs.addstr(2, 1, "starting up...".center(38), curses.color_pair(1))
        self.w_avgs.addstr(3, 1, " "*38, curses.color_pair(1))

        self.refresh()

    def __del__(self):
        self.stdscr.keypad(0)
        curses.echo()
        curses.nocbreak()
        curses.endwin()

    def refresh(self):
        for win in self.winlist:
            win.box()
        self.w_geo.addstr(0, 2, "Remote Location")
        self.w_proto.addstr(0, 2, "Protocols")
        self.w_afi.addstr(0, 2, "Address Family")
        self.w_avgs.addstr(0, 2, "Totals")
        self.w_dir.addstr(0, 2, "Direction")
        self.w_as.addstr(0, 2, "AS")
        self.w_peer.addstr(0, 2, "Peers")
        self.stdscr.refresh()
        for win in self.winlist:
            win.refresh()

    def clear_windows(self):
        for win in self.winlist:
            win.clear()

    def display(self, silo):
        # geo window
        sorted_geo = sorted(silo.c_geo.items(), key=lambda kv: kv[1], reverse=True)[:6]
        for num, (name, bitcount) in enumerate(sorted_geo, start=1):
            self.w_geo.addstr(num, 1, "{0: >7}: {1: >5.2f}% - {2:.2f} Mbit/s".format(name if name else 'other', bitcount/silo.c_bits['total']*100, bitcount/1024**2/silo.resolution))

        # proto window
        sorted_protos = sorted(silo.c_protos.items(), key=lambda kv: kv[1], reverse=True)[:5]
        for num, (name, bitcount) in enumerate(sorted_protos, start=1):
            self.w_proto.addstr(num, 1, "{0: >7}: {1: >5.2f}% - {2:.2f} Mbit/s".format(name if name else 'other', bitcount/silo.c_bits['total']*100, bitcount/1024**2/silo.resolution))

        # afi window
        try:
            self.w_afi.addstr(1, 1, "   IPv6: {0: >5.2f}% - {1:.2f} Mbit/s".format(silo.c_bits['v6']/silo.c_bits['total']*100, silo.c_bits['v6']/1024**2/silo.resolution))
            self.w_afi.addstr(2, 1, "   IPv4: {0: >5.2f}% - {1:.2f} Mbit/s".format(silo.c_bits['v4']/silo.c_bits['total']*100, silo.c_bits['v4']/1024**2/silo.resolution))
            self.w_afi.addstr(3, 1, "  other: {0: >5.2f}% - {1:.2f} Mbit/s".format(silo.c_bits['oafi']/silo.c_bits['total']*100, silo.c_bits['oafi']/1024**2/silo.resolution))
        except ZeroDivisionError:
            self.w_afi.addstr(1, 1, "no data, total is 0")


        # total window
        self.w_avgs.addstr(1, 1, "   Bits: {0:.2f} Mbit/s".format(silo.c_bits['total']/1024**2/silo.resolution))
        self.w_avgs.addstr(2, 1, "Packets: {0:.2f}/s".format(silo.c_packets['total']/silo.resolution))
        self.w_avgs.addstr(3, 1, "  Flows: {0:.2f}/s".format(silo.c_flows['total']/silo.resolution))

        # as window
        sorted_as = sorted(silo.c_as.items(), key=lambda kv: kv[1], reverse=True)[:6]
        for num, (name, bitcount) in enumerate(sorted_as, start=1):
            self.w_as.addstr(num, 1, "{0: >10}: {1: >5.2f}% - {2:.2f} Mbit/s".format(name[:10] if name else 'other', bitcount/silo.c_bits['total']*100, bitcount/1024**2/silo.resolution))

        # peer windows
        sorted_peer = sorted(silo.c_peer.items(), key=lambda kv: kv[1], reverse=True)[:6]
        for num, (name, bitcount) in enumerate(sorted_peer, start=1):
            self.w_peer.addstr(num, 1, "{0: >10}: {1: >5.2f}% - {2:.2f} Mbit/s".format(name[:10] if name else 'other', bitcount/silo.c_bits['total']*100, bitcount/1024**2/silo.resolution))

        # direction window
        try:
            self.w_dir.addstr(1, 1, "     In: {0: >5.2f}% - {1:.2f} Mbit/s".format(silo.c_bits['in']/silo.c_bits['total']*100, silo.c_bits['in']/1024**2/silo.resolution))
            self.w_dir.addstr(2, 1, "    Out: {0: >5.2f}% - {1:.2f} Mbit/s".format(silo.c_bits['out']/silo.c_bits['total']*100, silo.c_bits['out']/1024**2/silo.resolution))
            self.w_dir.addstr(3, 1, "unknown: {0: >5.2f}% - {1:.2f} Mbit/s".format(silo.c_bits['odir']/silo.c_bits['total']*100, silo.c_bits['odir']/1024**2/silo.resolution))
        except ZeroDivisionError:
            self.w_dir.addstr(1, 1, "no data, total is 0")
        self.refresh()
    # }}}


class Silo():
    # data structures {{{
    def __init__(self, resolution):
        self.resolution = resolution
        self.c_protos = defaultdict(int)
        self.c_flows = defaultdict(int)
        self.c_bits = defaultdict(int)
        self.c_packets = defaultdict(int)
        self.c_geo = defaultdict(int)
        self.c_as = defaultdict(int)
        self.c_peer = defaultdict(int)
        self.ases = {
                'limelight': 22822,
                'cloudflare': 13335,
                'hetzner': 24940,
                'netflix': 2906,
                'ovh': 16276,
                'github': 36459,
                'youtube': 36561,
                'amazon': 16509,
                'blizzard': 57976,
                'dtag': 3320,
                'shadowc': 64476,
                'steam': 32590,
                'vzffnrmoev': 202329,
                'microsoft': 8075,
                'spotify': 8403 }

    def reset(self):
        self.__init__(self.resolution)

    def ingest(self, flowmsg):
        self.c_protos[flowmsg.ProtoName] += flowmsg.Bytes*8
        self.c_flows['total'] += 1
        self.c_bits['total'] += flowmsg.Bytes*8
        self.c_packets['total'] += flowmsg.Packets

        if flowmsg.Etype == 0x0800:
            self.c_flows['v4'] += 1
            self.c_bits['v4'] += flowmsg.Bytes*8
            self.c_packets['v4'] += flowmsg.Packets
        elif flowmsg.Etype == 0x86dd:
            self.c_flows['v6'] += 1
            self.c_bits['v6'] += flowmsg.Bytes*8
            self.c_packets['v6'] += flowmsg.Packets
        else:
            self.c_flows['oafi'] += 1
            self.c_bits['oafi'] += flowmsg.Bytes*8
            self.c_packets['oafi'] += flowmsg.Packets

        if flowmsg.FlowDirection == 0:
            self.c_flows['in'] += 1
            self.c_bits['in'] += flowmsg.Bytes*8
            self.c_packets['in'] += flowmsg.Packets
            self.c_peer[flowmsg.SrcIfDesc] += flowmsg.Bytes*8
        elif flowmsg.FlowDirection == 1:
            self.c_flows['out'] += 1
            self.c_bits['out'] += flowmsg.Bytes*8
            self.c_packets['out'] += flowmsg.Packets
            self.c_peer[flowmsg.DstIfDesc] += flowmsg.Bytes*8
        else:
            self.c_flows['odir'] += 1
            self.c_bits['odir'] += flowmsg.Bytes*8
            self.c_packets['odir'] += flowmsg.Packets

        for name, asn in self.ases.items():
            if flowmsg.SrcAS == asn or flowmsg.DstAS == asn:
                self.c_as[name] += flowmsg.Bytes*8
                break
        else:
            self.c_as['other'] += flowmsg.Bytes*8


        self.c_geo[flowmsg.RemoteCountry] += flowmsg.Bytes*8
    # }}}

if __name__ == "__main__":
    seconds = 1
    ips = []
    if len(sys.argv) > 1:
        try:
            seconds = int(sys.argv[1])
            import ipaddress
            for ipstr in sys.argv[2:]:
                ips.append(ipaddress.ip_address(ipstr).packed)
        except (ValueError, IndexError):
            pass
    try:
        scr = Screen()
        silo = Silo(seconds)

        while True:
            # collect data first
            finish_time = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
            while datetime.datetime.now() < finish_time:
                raw = consumer.poll()
                if raw.error():
                    continue
                flowmsg = api.FlowMessage()
                flowmsg.ParseFromString(raw.value())

                if ips and not any([flowmsg.SrcAddr == ip or flowmsg.DstAddr == ip for ip in ips]):
                    continue
                silo.ingest(flowmsg)

            # print data
            scr.clear_windows()
            scr.display(silo)
            silo.reset()
    except KeyboardInterrupt:
        sys.exit(0)
