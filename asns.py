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

a = {
		70:     "National Library Medicine USA",
		43:     "Brookhaven National Laboratory",
		174:    "Cogent",
		513:    "CERN",
		559:    "SWITCH",
		680:    "DFN",
		702:    "Verizon",
		714:    "Apple",
		786:    "JANET",
		1239:   "Sprint",
		1273:   "Vodafone",
		1297:   "CERN",
		1299:   "Telia",
		1754:   "DESY",
		2018:   "AFRINIC",
		2603:   "NORDUnet",
		2906:   "Netflix",
		2914:   "NTT",
		3209:   "Vodafone",
		3257:   "GTT",
		3303:   "Swisscom",
		3320:   "Deutsche Telekom",
		3356:   "CenturyLink",
		4356:   "Epic Games",
		5430:   "freenet",
		5501:   "Fraunhofer",
		5511:   "Orange",
		6185:   "Apple",
		6453:   "TATA",
		6507:   "Riot Games",
		6724:   "Strato",
		6735:   "sdt.net",
		6805:   "Telefonica",
		6830:   "Vodafone",
		6939:   "Hurricane Electric",
		7018:   "AT&T",
		8068:   "Microsoft",
		8075:   "Microsoft",
		8220:   "Colt",
		8403:   "Spotify",
		8560:   "1&1",
		8674:   "Netnod",
		8763:   "DENIC",
		8881:   "Versatel",
		9009:   "GLOBALAXS",
		10310:  "Yahoo",
		13030:  "Init7",
		13335:  "Cloudflare",
		15133:  "Verizon",
		15169:  "Google",
		16276:  "OVH",
		16509:  "Amazon",
		16625:  "Akamai",
		19679:  "Dropbox",
		20446:  "Highwinds",
		20504:  "RTL",
		20677:  "imos",
		20940:  "Akamai",
		22822:  "Limelight",
		24940:  "Hetzner",
		30361:  "Swiftwill",
		31334:  "Kabel Deutschland",
		32590:  "Valve Steam",
		32934:  "Facebook",
		33915:  "Vodafone",
		35402:  "ecotel",
		36459:  "Github",
		36561:  "Youtube",
		39702:  "S-IT",
		41552:  "Ebay",
		41690:  "Dailymotion",
		46489:  "Twitch",
		48918:  "Globalways",
		54113:  "Fastly",
		54994:  "QUANTIL",
		57976:  "Blizzard",
		58069:  "KIT",
		60781:  "Leaseweb",
		61339:  "LHC",
		197540: "Netcup",
		197602: "TV-9",
		206339: "Schuler Pressen",
}
b = defaultdict(int)
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
        if flowmsg.FlowDirection == 0 and flowmsg.SrcAS not in a.keys():
            if flowmsg.SrcAS > 64512 and flowmsg.SrcAS < 65534:
                print(flowmsg)
            b[flowmsg.SrcAS] += flowmsg.Bytes
        elif flowmsg.FlowDirection == 1 and flowmsg.DstAS not in a.keys():
            if flowmsg.DstAS > 64512 and flowmsg.DstAS < 65534:
                print(flowmsg)
            b[flowmsg.DstAS] += flowmsg.Bytes
except KeyboardInterrupt:
    ranked = sorted(b.items(), key=lambda kv: kv[1], reverse=True)[:50]
    for (asn, vol) in ranked:
        print("as{}".format(asn), vol/1024**2)
consumer.close()
