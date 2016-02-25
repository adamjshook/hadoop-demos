import avro.schema, json, logging, rpyc, sys
from avro.io import AvroTypeException, BinaryEncoder, DatumWriter
from kafka import KafkaProducer
from rpyc.utils.server import ThreadedServer
from StringIO import StringIO

# Parse the Avro schema and create a DatumWriter
schema = avro.schema.parse(open("match.avsc").read())
writer = DatumWriter(writers_schema=schema)


def __init_logging():
    """Initialized the logging module for the RPYC Server"""
    root = logging.getLogger()
    root.setLevel(logging.getLevelName("INFO"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(ch)



class LolMatchData(rpyc.Service):
    """rpyc Service, delivered JSON messages of LoL match data"""

    _topic = None  # The topic to post messages to
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
            avro_obj = dict()
            avro_obj["mapId"] = data["mapId"]
            avro_obj["matchCreation"] = data["matchId"]
            avro_obj["matchDuration"] = data["matchDuration"]
            avro_obj["matchId"] = data["matchId"]
            avro_obj["matchMode"] = data["matchMode"]
            avro_obj["winningTeam"] = data["teams"][0]["teamId"] if data["teams"][0]["winner"] else data["teams"][1]["teamId"]
            avro_obj["participants"] = []
            avro_obj["teams"] = []

            # Append a participant for each one in the match JSON data
            for pData in data["participants"]:
                p = dict()
                p["championId"] = pData["championId"]
                p["teamId"] = pData["teamId"]
                p["winner"] = pData["teamId"] == avro_obj["winningTeam"]

                pId = pData["participantId"]

                # Loop through the participantIdentities in the match JSON for the correct summoner ID and name
                for pIdData in data["participantIdentities"]:
                    if pIdData["participantId"] == pId:
                        p["summonerId"] = pIdData["player"]["summonerId"]
                        p["summonerName"] = pIdData["player"]["summonerName"]
                        break

                avro_obj["participants"].append(p)

            # Append a participant for each one in the match JSON data
            for tData in data["teams"]:
                t = dict()
                t["teamId"] = tData["teamId"]
                t["winner"] = tData["winner"]
                t["firstInhibitor"] = tData["firstInhibitor"]
                t["firstBlood"] = tData["firstBlood"]
                t["firstTower"] = tData["firstTower"]

                avro_obj["teams"].append(t)

            # Create a string stream and encoder
            stream = StringIO()
            encoder = BinaryEncoder(stream)

            # Encode the avro object
            writer.write(avro_obj, encoder)

            # Send the encoded data to the producer and flush the message
            self._producer.send(self._topic, stream.getvalue())
            self._producer.flush()

            # Log the object we wrote
            print "Wrote %s" % avro_obj

        except AvroTypeException as e:
            print e
        except:
            print str(sys.exc_info()[0])

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "usage: python producer.py <server.port> <brokers> <topic>"
        print "    server.port - port to bind to for rpyc calls"
        print "    brokers - comma-delimited list of host:port pairs for Kafka brokers"
        print "    topic - Kafka topic to post messages to, must exist"
        sys.exit(1)

    __init_logging()

    port = int(sys.argv[1])
    brokers = sys.argv[2]
    topic = sys.argv[3]

    LolMatchData._producer = KafkaProducer(bootstrap_servers=brokers)
    LolMatchData._topic = topic

    ThreadedServer(LolMatchData, port=port).start()
