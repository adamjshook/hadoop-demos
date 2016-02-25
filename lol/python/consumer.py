#!/usr/bin/python

import avro, subprocess, sys

from avro.datafile import DataFileWriter
from avro.io import BinaryDecoder, DatumReader, DatumWriter
from kafka import KafkaConsumer
from StringIO import StringIO
from time import strftime, gmtime
from tempfile import NamedTemporaryFile

# Parse the avro schema and reader
schema = avro.schema.parse(open("match.avsc").read())
rdr = DatumReader(writers_schema=schema)


def __move_to_hdfs(src, dest):
    """Moves the given src file to the destination path in HDFS

    Args:
        src (str): The local file to be moved
        dest (str): The destination file, an HDFS path
    """
    (dir, file) = dest
    print "Moving %s to %s" % (src, "%s/%s" % (dir, file))
    subprocess.call(['hdfs', 'dfs', '-mkdir', '-p', dir])
    subprocess.call(['hdfs', 'dfs', '-moveFromLocal', src, "%s/%s" % (dir, file)])


def __dest_filename(outputDir):
    """Create a formatted output file name based on the current time

    Args:
        outputDir (str): The root output directory of HDFS
    """
    return ("%s/%s" % (outputDir, strftime("%Y/%m/%d/%H", gmtime())), strftime("data-%s.avro", gmtime()))


def __new_writer():
    """Creates a new DataFileWriter at a temporary location

    Returns:
        (DataFileWriter, str): The writer and the filename for where the data is being written
     """
    file = NamedTemporaryFile(delete=False)
    return (DataFileWriter(file, DatumWriter(), schema), file.name)


def __decode(msg):
    """Decodes the given binary Avro message, returning the Avro object as a dict

    Args:
        msg (str): The binary Avro message to decode
    """
    encoder = BinaryDecoder(StringIO(msg))
    return rdr.read(encoder)


def __consume(brokers, outputDir, messagesPerFile):
    """Continuously consumes messages from the Kafka broker, appending them to a temporary file until
     messagesPerFile have been appended.  The file is then rolled over, being written to HDFS
     under the output directory at outputDir/YYYY/mm/dd/HH/data-%s.avro

    Args:
        brokers (str): Comma-delimited list of Kafka brokers
        outputDir (str): The HDFS root directory to put data files
        messagesPerFile (int): The number of messages to write per file
    """
    # Create the KafkaConsumer
    consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

    # Create a new writer
    (writer, filename) = __new_writer()

    # For each new message
    numMessages = 0
    for msg in consumer:
        # Increment number of messages by one
        numMessages += 1

        # Decode the message
        user = __decode(msg.value)

        # Print the user to stdout
        print user

        # Append the user to the writer
        writer.append(user)

        # If we have hit the number of messages per file
        if numMessages == messagesPerFile:
            print "Received %s messages, rolling file %s" % (numMessages, filename)

            # Flush and close the writer
            writer.flush()
            writer.close()

            # Write the data to HDFS
            __move_to_hdfs(filename, __dest_filename(outputDir))

            # Create a new writer
            (writer, filename) = __new_writer()

            # Reset message counter
            numMessages = 0

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print "usage: python consumer.py <brokers> <topic> <output> <messages.per.file>"
        print "    brokers - comma-delimited list of host:port pairs for Kafka brokers"
        print "    topic - Kafka topic to post messages to, must exist"
        print "    output - Absolute HDFS directory to write files to, e.g. /in/lol"
        print "    messages.per.file - Number of messages to write to a single file before rolling over"
        sys.exit(1)

    brokers = sys.argv[1]
    topic = sys.argv[2]
    output = sys.argv[3]
    messagesPerFile = int(sys.argv[4])

    # Start to consume messages
    __consume(brokers, output, messagesPerFile)