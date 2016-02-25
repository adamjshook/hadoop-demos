#!/usr/bin/python

import avro, subprocess, sys

from avro.datafile import DataFileWriter
from avro.io import BinaryDecoder, DatumReader, DatumWriter
from kafka import KafkaConsumer
from StringIO import StringIO
from time import strftime, gmtime
from tempfile import NamedTemporaryFile

schema = avro.schema.parse(open("match.avsc").read())
rdr = DatumReader(writers_schema=schema)

def __write_data(src, dest):
    (dir, file) = dest
    print "Moving %s to %s" % (src, "%s/%s" % (dir, file))
    subprocess.call(['hdfs', 'dfs', '-mkdir', '-p', dir])
    subprocess.call(['hdfs', 'dfs', '-moveFromLocal', src, "%s/%s" % (dir, file)])

def __dest_filename(outputDir):
    return ("%s/%s" % (outputDir, strftime("%Y/%m/%d/%H", gmtime())), strftime("data-%s.avro", gmtime()))

def __new_writer():
    file = NamedTemporaryFile(delete=False)
    return (DataFileWriter(file, DatumWriter(), schema), file.name)

def __decode(msg):
    encoder = BinaryDecoder(StringIO(msg))
    return rdr.read(encoder)

def __consume(consumer, outputDir, messagesPerFile=10):
    numMessages = 0
    (writer, filename) = __new_writer()
    for msg in consumer:
        numMessages += 1
        user = __decode(msg.value)
        print user
        writer.append(user)

        if numMessages == messagesPerFile:
            print "Received %s messages, rolling file %s" % (numMessages, filename)
            writer.flush()
            writer.close()
            __write_data(filename, __dest_filename(outputDir))
            (writer, filename) = __new_writer()
            numMessages = 0

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "usage: python consumer.py <brokers> <topic> <output> <messages.per.file>"
        sys.exit(1)

    brokers = sys.argv[1]
    topic = sys.argv[2]
    output = sys.argv[3]

    consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

    __consume(consumer, output)
