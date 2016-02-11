#!/usr/bin/python

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(0,100):
	producer.send('pyfoo', b'Today is brought to you by the number %d' % i)

# Close your producer to sync the messages!
producer.close()
