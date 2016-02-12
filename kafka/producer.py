#!/usr/bin/python

import sys
from kafka import KafkaProducer

if len(sys.argv) != 3:
	print "usage: python producer.py <brokers> <topic>"
	sys.exit(1)

brokers = sys.argv[1]
topic = sys.argv[2]

producer = KafkaProducer(bootstrap_servers=brokers)

print "Producing messages..."
for i in range(0,100):
	producer.send(topic, b'Today is brought to you by the number %d' % i)

print "Done."

# Close your producer to sync the messages!
producer.close()
