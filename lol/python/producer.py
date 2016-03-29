import avro.schema, json, logging, rpyc, sys
from avro.io import AvroTypeException, BinaryEncoder, DatumWriter
from kafka import KafkaProducer
from rpyc.utils.server import ThreadedServer
from StringIO import StringIO
from uuid import uuid4

# Parse the Avro schema and create a DatumWriter
match_schema = avro.schema.parse(open("match.avsc").read())
match_writer = DatumWriter(writers_schema=match_schema)
part_schema = avro.schema.parse(open("participant.avsc").read())
part_writer = DatumWriter(writers_schema=part_schema)


def __init_logging():
    """Initialized the logging module for the RPYC Server"""
    root = logging.getLogger()
    root.setLevel(logging.getLevelName("INFO"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(ch)



class LolMatchData(rpyc.Service):
    """rpyc Service, delivered JSON messages of LoL match data"""

    _match_topic = None  # The topic to post messages to
    _participant_topic = None  # The topic to post messages to
    _producer = None  # The Kafka producer

    def exposed_match(self, dataStr):
        """
        Exposed rpyc function for posting a LoL match JSON string

        :param dataStr: The JSON data string
        """
        try:
            # Parse the JSON into a dict
            data = json.loads(dataStr)

            # Begin construction of Avro object from the JSON data
            match_obj = dict()
            match_obj["mapId"] = data["mapId"]
            match_obj["matchCreation"] = data["matchId"]
            match_obj["matchDuration"] = data["matchDuration"]
            match_obj["matchId"] = data["matchId"]
            match_obj["matchMode"] = data["matchMode"]
            match_obj["winningTeam"] = data["teams"][0]["teamId"] if data["teams"][0]["winner"] else data["teams"][1]["teamId"]
            match_obj["participants"] = []

            firstInhibitor= data["teams"][0]["teamId"] if data["teams"][0]["firstInhibitor"] else data["teams"][1]["teamId"]
            firstBlood = data["teams"][0]["teamId"] if data["teams"][0]["firstBlood"] else data["teams"][1]["teamId"]
            firstTower = data["teams"][0]["teamId"] if data["teams"][0]["firstTower"] else data["teams"][1]["teamId"]

            # Append a participant for each one in the match JSON data
            for pData in data["participants"]:
                part_obj = dict()
                part_obj["uuid"] = uuid4().hex
                part_obj["matchId"] = data["matchId"]
                part_obj["championId"] = pData["championId"]
                part_obj["teamId"] = pData["teamId"]
                part_obj["winner"] = pData["teamId"] == match_obj["winningTeam"]
                part_obj["firstInhibitor"] = pData["teamId"] == firstInhibitor
                part_obj["firstBlood"] = pData["teamId"] == firstBlood
                part_obj["firstTower"] = pData["teamId"] == firstTower

                pId = pData["participantId"]

                # Loop through the participantIdentities in the match JSON for the correct summoner ID and name
                for pIdData in data["participantIdentities"]:
                    if pIdData["participantId"] == pId:
                        part_obj["summonerId"] = pIdData["player"]["summonerId"]
                        part_obj["summonerName"] = pIdData["player"]["summonerName"]
                        break

                match_obj["participants"].append(part_obj["uuid"])

                # Create a string stream and encoder
                stream = StringIO()
                encoder = BinaryEncoder(stream)

                # Encode the avro object
                part_writer.write(part_obj, encoder)

                # Send the encoded data to the producer and flush the message
                self._producer.send(self._participant_topic, stream.getvalue())
                self._producer.flush()
                print "Wrote %s" % part_obj

            # Create a string stream and encoder
            stream = StringIO()
            encoder = BinaryEncoder(stream)

            # Encode the avro object
            match_writer.write(match_obj, encoder)

            # Send the encoded data to the producer and flush the message
            self._producer.send(self._match_topic, stream.getvalue())
            self._producer.flush()

            # Log the object we wrote
            print "Wrote %s" % match_obj

        except AvroTypeException as e:
            print e
        except:
            print str(sys.exc_info()[0])

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print "usage: python producer.py <server.port> <brokers> <match_topic> <participant_topic>"
        print "    server.port - port to bind to for rpyc calls"
        print "    brokers - comma-delimited list of host:port pairs for Kafka brokers"
        print "    match_topic - Kafka topic to post match data to, must exist"
        print "    participant_topic - Kafka topic to post participant data to, must exist"
        sys.exit(1)

    __init_logging()

    port = int(sys.argv[1])
    brokers = sys.argv[2]
    match_topic = sys.argv[3]
    participant_topic = sys.argv[4]

    LolMatchData._producer = KafkaProducer(bootstrap_servers=brokers)
    LolMatchData._match_topic = match_topic
    LolMatchData._participant_topic = participant_topic

    ThreadedServer(LolMatchData, port=port).start()
