#!/usr/bin/env python3
import confluent_kafka
import flow_messages_enriched_pb2 as api # this needs to be in the local path
import helpers as h # this needs to be in the local path

import multiprocessing
import signal
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

def go_to_oldest(consumer, partitions):
    for p in partitions:
        p.offset = confluent_kafka.OFFSET_BEGINNING
    consumer.assign(partitions)

class ConsumerProc(multiprocessing.Process):
    def __init__(self, idx, lock):
        super(ConsumerProc, self).__init__()
        self.id = idx
        self.lock = lock
        self.lag = 0.0
        self.count = 0
        signal.signal(signal.SIGTERM, self._endloop)
        self.consumer = confluent_kafka.Consumer(
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
        self.consumer.subscribe(['flow-messages-enriched'], on_assign=self._go_to_newest)

    def _go_to_newest(self, consumer, partitions):
        p = partitions[self.id]
        p.offset = confluent_kafka.OFFSET_END
        self.consumer.assign([p])

    def _endloop(self):
        self._running = False

    def run(self):
        self._running = True
        self.lag = 0.0
        self.count = 0
        while self._running:
            raw = self.consumer.poll()
            if raw.error():
                continue
            now = int(time.time())
            flowmsg = api.FlowMessage()
            flowmsg.ParseFromString(raw.value())
            self.lock.acquire()
            self.lag += now - flowmsg.TimeFlowEnd
            self.count += 1
            self.lock.release()
        self.consumer.close()


if __name__ == "__main__":
    NUMBER_OF_PROCESSES = 3
    processes = []
    for i in range(NUMBER_OF_PROCESSES):
        p = ConsumerProc(i, multiprocessing.Lock())
        processes.append(p)
        p.start()

    try:
        while True:
            lag = 0.0
            count = 0
            for proc in processes:
                proc.lock.acquire()
                lag += proc.lag
                count += proc.count
                try:
                    proc.lag = proc.lag/proc.count
                    proc.count = 1
                except ZeroDivisionError:
                    pass
                proc.lock.release()
            sys.stdout.write('\x0d')
            if count != 0:
                sys.stdout.write("{:.2f}".format(lag/count))
            else:
                print("no data")
            sys.stdout.flush()
            time.sleep(1.0)
    except KeyboardInterrupt:
        for proc in processes:
            try:
                proc.lock.release()
            except ValueError:
                pass
            proc.terminate()
        for proc in processes:
            proc.join()
