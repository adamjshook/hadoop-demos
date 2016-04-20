#!/usr/bin/python

import avro, subprocess, sys, time

from avro.datafile import DataFileWriter
from avro.io import BinaryDecoder, DatumReader, DatumWriter
from kafka import KafkaConsumer
from StringIO import StringIO
from time import strftime, gmtime
from tempfile import NamedTemporaryFile

def __move_to_hdfs(src, dest):
    """
    Moves the given src file to the destination path in HDFS

    :param src: The local file to be moved
    :param dest: The destination file, an HDFS path
    """
    (dir, file) = dest
    print "Moving %s to %s" % (src, "%s/%s" % (dir, file))
    subprocess.call(['hdfs', 'dfs', '-mkdir', '-p', dir])
    subprocess.call(['hdfs', 'dfs', '-moveFromLocal', src, "%s/%s" % (dir, file)])


def __dest_filename(outputDir):
    """
    Create a formatted output file name based on the current time

    :param outputDir: The root output directory of HDFS
    """
    return ("%s/%s" % (outputDir, strftime("%Y/%m/%d/%H", gmtime())), strftime("data-%s.avro", gmtime()))


def __new_writer():
    """
    Creates a new DataFileWriter at a temporary location

    :return: (DataFileWriter, str): The writer and the filename for where the data is being written
    """
    file = NamedTemporaryFile(delete=False)
    return (DataFileWriter(file, DatumWriter(), schema), file.name)


def __decode(msg, rdr):
    """
    Decodes the given binary Avro message, returning the Avro object as a dict

    :param msg: The binary Avro message to decode
    """
    encoder = BinaryDecoder(StringIO(msg))
    return rdr.read(encoder)


def __consume(brokers, outputDir, messagesPerFile, schema, secondsPerFile):
    """
    Continuously consumes messages from the Kafka broker, appending them to a temporary file until
     messagesPerFile have been appended.  The file is then rolled over, being written to HDFS
     under the output directory at outputDir/YYYY/mm/dd/HH/data-%s.avro

    :param brokers: Comma-delimited list of Kafka brokers
    :param outputDir: The HDFS root directory to put data files
    :param messagesPerFile: The number of messages to write per file
    """
    rdr = DatumReader(writers_schema=schema)

    # Create the KafkaConsumer
    consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

    # Create a new writer
    (writer, filename) = __new_writer()

    # For each new message
    numMessages = 0
    future = time.time() + secondsPerFile
    for msg in consumer:
        # Increment number of messages by one
        numMessages += 1

        # Decode the message
        user = __decode(msg.value, rdr)

        # Append the user to the writer
        writer.append(user)

        # If we have hit the number of messages per file or the time limit
        if numMessages == messagesPerFile or time.time() >= future:
            print "Received %s messages, rolling file %s" % (numMessages, filename)

            # Flush and close the writer
            writer.flush()
            writer.close()

            # Write the data to HDFS
            __move_to_hdfs(filename, __dest_filename(outputDir))

            # Create a new writer
            (writer, filename) = __new_writer()

            # Reset rollover criteria
            numMessages = 0
            future = time.time() + secondsPerFile

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print "usage: python consumer.py <brokers> <topic> <output> <messages.per.file> <seconds.per.file>"
        print "    brokers - comma-delimited list of host:port pairs for Kafka brokers"
        print "    topic - Kafka topic to post messages to, must exist"
        print "    output - Absolute HDFS directory to write files to, e.g. /in/lol"
        print "    messages.per.file - Number of messages to write to a single file before rolling over"
        print "    seconds.per.file - Number of seconds that pass before rolling over"
        sys.exit(1)

    brokers = sys.argv[1]
    topic = sys.argv[2]
    output = sys.argv[3]
    messagesPerFile = int(sys.argv[4])
    secondsPerFile = int(sys.argv[5])

    # Parse the avro schema and reader
    schema = avro.schema.parse(open("tweet.avsc").read())

    # Start to consume messages
    __consume(brokers, output, messagesPerFile, schema, secondsPerFile)
