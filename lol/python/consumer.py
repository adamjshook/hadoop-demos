#!/usr/bin/python

import avro, subprocess, sys

from avro.datafile import DataFileWriter
from avro.io import BinaryDecoder, DatumReader, DatumWriter
from kafka import KafkaConsumer
from StringIO import StringIO
from time import strftime, gmtime
from tempfile import NamedTemporaryFile

# Parse the avro schema and reader
match_schema = avro.schema.parse(open("match.avsc").read())
match_rdr = DatumReader(writers_schema=match_schema)
part_schema = avro.schema.parse(open("participant.avsc").read())
part_rdr = DatumReader(writers_schema=part_schema)

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


def __dest_filename(outputDir, dataSet):
    """
    Create a formatted output file name based on the current time

    :param outputDir: The root output directory of HDFS
    :param dataSet: The data set, match or participants
    """
    return ("%s/%s/%s" % (outputDir, dataSet, strftime("%Y/%m/%d/%H", gmtime())), strftime("data-%s.avro", gmtime()))


def __new_writer(schema):
    """
    Creates a new DataFileWriter at a temporary location

    :return: (DataFileWriter, str): The writer and the filename for where the data is being written
    """
    file = NamedTemporaryFile(delete=False)
    return (DataFileWriter(file, DatumWriter(), schema), file.name)


def __decode(rdr, msg):
    """
    Decodes the given binary Avro message, returning the Avro object as a dict

    :param rdr: The reader to use for decoding
    :param msg: The binary Avro message to decode
    """
    encoder = BinaryDecoder(StringIO(msg))
    return rdr.read(encoder)

def __consume(brokers, outputDir, messagesPerFile):
    """
    Continuously consumes messages from the Kafka broker, appending them to a temporary file until
     messagesPerFile have been appended.  The file is then rolled over, being written to HDFS
     under the output directory at outputDir/YYYY/mm/dd/HH/data-%s.avro

    :param brokers: Comma-delimited list of Kafka brokers
    :param outputDir: The HDFS root directory to put data files
    :param messagesPerFile: The number of messages to write per file
    """
    # Create the KafkaConsumer
    consumer = KafkaConsumer(match_topic, participant_topic, bootstrap_servers=brokers)

    # Create a new writer
    (match_writer, match_filename) = __new_writer(match_schema)
    (part_writer, part_filename) = __new_writer(part_schema)

    # For each new message
    numMessages = 0
    for msg in consumer:
        try:
            print msg
            # Increment number of messages by one
            numMessages += 1

            if msg.topic == match_topic:
                # Decode the message
                match = __decode(match_rdr, msg.value)

                # Print the user to stdout
                print match

                # Append the user to the writer
                match_writer.append(match)

                # If we have hit the number of messages per file
                if numMessages == messagesPerFile:
                    print "Received %s messages, rolling file %s" % (numMessages, match_filename)

                    # Flush and close the writer
                    match_writer.flush()
                    match_writer.close()

                    # Write the data to HDFS
                    __move_to_hdfs(match_filename, __dest_filename(outputDir, "match"))

                    # Create a new writer
                    (match_writer, match_filename) = __new_writer(match_schema)

                    # Flush and close the writer
                    part_writer.flush()
                    part_writer.close()

                    # Write the data to HDFS
                    __move_to_hdfs(part_filename, __dest_filename(outputDir, "participants"))

                    # Create a new writer
                    (part_writer, part_filename) = __new_writer(part_schema)

                    # Reset message counter
                    numMessages = 0
            else:
                # Decode the message
                part = __decode(part_rdr, msg.value)

                # Print the user to stdout
                print part

                # Append the user to the writer
                part_writer.append(part)

                # If we have hit the number of messages per file
                if numMessages == messagesPerFile:
                    print "Received %s messages, rolling file %s" % (numMessages, match_filename)

                    # Flush and close the writer
                    match_writer.flush()
                    match_writer.close()

                    # Write the data to HDFS
                    __move_to_hdfs(match_filename, __dest_filename(outputDir, "match"))

                    # Create a new writer
                    (match_writer, match_filename) = __new_writer(match_schema)

                    # Flush and close the writer
                    part_writer.flush()
                    part_writer.close()

                    # Write the data to HDFS
                    __move_to_hdfs(part_filename, __dest_filename(outputDir, "participants"))

                    # Create a new writer
                    (part_writer, part_filename) = __new_writer(part_schema)

                    # Reset message counter
                    numMessages = 0
        except UnicodeDecodeError as e:
            print e

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print "usage: python consumer.py <brokers> <match_topic> <participant_topic> <output> <messages.per.file>"
        print "    brokers - comma-delimited list of host:port pairs for Kafka brokers"
        print "    match_topic - Kafka topic to read match data to, must exist"
        print "    participant_topic - Kafka topic to read participant data to, must exist"
        print "    output - Absolute HDFS directory to write files to, e.g. /in/lol"
        print "    messages.per.file - Number of messages to write to a single file before rolling over"
        sys.exit(1)

    brokers = sys.argv[1]
    match_topic = sys.argv[2]
    participant_topic = sys.argv[3]
    output = sys.argv[4]
    messagesPerFile = int(sys.argv[5])

    # Start to consume messages
    __consume(brokers, output, messagesPerFile)