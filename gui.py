#!/usr/bin/env python3
import sys
import datetime
import curses
from curses import wrapper
from collections import defaultdict

from confluent_kafka import Consumer
from ssl import get_default_verify_paths
import flow_messages_enriched_pb2 as api # this needs to be in the local path

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


def main(stdscr, seconds=1, ips=[]):
    stdscr.clear()
    curses.curs_set(0)

    w_geo = curses.newwin(6+2, 40, 0, 0)
    w_afi = curses.newwin(3+2, 40, 0, 40)
    w_avgs = curses.newwin(3+2, 40, 5, 40)
    w_dir = curses.newwin(3+2, 40, 10, 40)
    w_proto = curses.newwin(5+2, 40, 8, 0)
    winlist = [w_geo, w_proto, w_afi, w_avgs, w_dir]
    w_geo.addstr(1, 1, "starting up...")
    for win in winlist:
        win.box()
    stdscr.refresh()
    for win in winlist:
        win.refresh()

    c_protos = defaultdict(int)
    c_flows = defaultdict(int)
    c_bits = defaultdict(int)
    c_packets = defaultdict(int)
    c_geo = defaultdict(int)

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
            c_protos[flowmsg.ProtoName] += flowmsg.Bytes*8
            c_flows['total'] += 1
            c_bits['total'] += flowmsg.Bytes*8
            c_packets['total'] += flowmsg.Packets

            if flowmsg.Etype == 0x0800:
                c_flows['v4'] += 1
                c_bits['v4'] += flowmsg.Bytes*8
                c_packets['v4'] += flowmsg.Packets
            elif flowmsg.Etype == 0x86dd:
                c_flows['v6'] += 1
                c_bits['v6'] += flowmsg.Bytes*8
                c_packets['v6'] += flowmsg.Packets
            else:
                c_flows['oafi'] += 1
                c_bits['oafi'] += flowmsg.Bytes*8
                c_packets['oafi'] += flowmsg.Packets

            if flowmsg.FlowDirection == 0:
                c_flows['in'] += 1
                c_bits['in'] += flowmsg.Bytes*8
                c_packets['in'] += flowmsg.Packets
            elif flowmsg.FlowDirection == 1:
                c_flows['out'] += 1
                c_bits['out'] += flowmsg.Bytes*8
                c_packets['out'] += flowmsg.Packets
            else:
                c_flows['odir'] += 1
                c_bits['odir'] += flowmsg.Bytes*8
                c_packets['odir'] += flowmsg.Packets

            c_geo[flowmsg.RemoteCountry] += flowmsg.Bytes*8

        # print data
        for win in winlist:
            win.clear()
            win.box()
        sorted_geo = sorted(c_geo.items(), key=lambda kv: kv[1], reverse=True)[:6]
        w_geo.addstr(0, 2, "Remote Location")
        for num, (name, bytecount) in enumerate(sorted_geo, start=1):
            w_geo.addstr(num, 1, "{0}: {1:.2f} Mbit/s".format(name if name else 'other', bytecount/1024**2/seconds))

        sorted_protos = sorted(c_protos.items(), key=lambda kv: kv[1], reverse=True)[:5]
        w_proto.addstr(0, 2, "Protocols")
        for num, (name, bytecount) in enumerate(sorted_protos, start=1):
            w_proto.addstr(num, 1, "{0: <8}: {1:.2f} Mbit/s".format(name if name else 'other', bytecount/1024**2/seconds))

        w_afi.addstr(0, 2, "Address Family")
        try:
            w_afi.addstr(1, 1, " IPv6: {0: >5.2f}% - {1:.2f} Mbit/s".format(c_bits['v6']/c_bits['total']*100, c_bits['v6']/1024**2/seconds))
            w_afi.addstr(2, 1, " IPv4: {0: >5.2f}% - {1:.2f} Mbit/s".format(c_bits['v4']/c_bits['total']*100, c_bits['v4']/1024**2/seconds))
            w_afi.addstr(3, 1, "other: {0: >5.2f}% - {1:.2f} Mbit/s".format(c_bits['oafi']/c_bits['total']*100, c_bits['oafi']/1024**2/seconds))
        except ZeroDivisionError:
            w_afi.addstr(1, 1, "no data, total is 0")


        w_avgs.addstr(0, 2, "Totals")
        w_avgs.addstr(1, 1, "   Bits: {0:.2f} Mbit/s".format(c_bits['total']/1024**2/seconds))
        w_avgs.addstr(2, 1, "Packets: {0:.2f}/s".format(c_packets['total']/seconds))
        w_avgs.addstr(3, 1, "  Flows: {0:.2f}/s".format(c_flows['total']/seconds))

        w_dir.addstr(0, 2, "Direction")
        try:
            w_dir.addstr(1, 1, " In: {0: >5.2f}% - {1:.2f} Mbit/s".format(c_bits['in']/c_bits['total']*100, c_bits['in']/1024**2/seconds))
            w_dir.addstr(2, 1, "Out: {0: >5.2f}% - {1:.2f} Mbit/s".format(c_bits['out']/c_bits['total']*100, c_bits['out']/1024**2/seconds))
            w_dir.addstr(3, 1, "N/A: {0: >5.2f}% - {1:.2f} Mbit/s".format(c_bits['odir']/c_bits['total']*100, c_bits['odir']/1024**2/seconds))
        except ZeroDivisionError:
            w_dir.addstr(1, 1, "no data, total is 0")

        stdscr.refresh()
        for win in winlist:
            win.refresh()

        # clear data structs
        c_protos = defaultdict(int)
        c_flows = defaultdict(int)
        c_bits = defaultdict(int)
        c_packets = defaultdict(int)
        c_geo = defaultdict(int)

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
    wrapper(main, seconds, ips)
except KeyboardInterrupt:
    pass
