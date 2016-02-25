#!/usr/bin/python

import sys
from kafka import KafkaConsumer

def consume(consumer):
    for msg in consumer:
        print msg.value

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: python consumer.py <brokers> <topic>"
        sys.exit(1)

    brokers = sys.argv[1]
    topic = sys.argv[2]

    consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

    consume(consumer)
