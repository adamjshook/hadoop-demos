import rpyc
from rpyc.utils.server import ForkingServer
import sys
import json
import logging
from StringIO import StringIO
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from avro.io import AvroTypeException

schema = avro.schema.parse(open("match.avsc").read())

def __init_logging():
    root = logging.getLogger()
    root.setLevel(logging.getLevelName("INFO"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(ch)

class LolMatchData(rpyc.Service):
    def exposed_match(self, dataStr):
        try:
            data = json.loads(dataStr)

            print "Creating Avro object..."
            avroObject = { }
            avroObject["mapId"] = data["mapId"]
            avroObject["matchCreation"] = data["matchId"]
            avroObject["matchDuration"] = data["matchDuration"]
            avroObject["matchId"] = data["matchId"]
            avroObject["matchMode"] = data["matchMode"]
            avroObject["winningTeam"] = data["teams"][0]["teamId"] if data["teams"][0]["winner"] else data["teams"][1]["teamId"]
            avroObject["participants"] = []
            avroObject["teams"] = []

            print "Writing Avro object..."
            stream = StringIO()
            writer = DataFileWriter(stream, DatumWriter(), schema)
            writer.append(avroObject)
            writer.flush()
            print "Data is: %s" % stream.getvalue()
            print "Done"
            writer.close()
        except AvroTypeException as e:
            print e
        except ValueError as e:
            print e
        except:
            print str(sys.exc_info()[0])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "usage: python producer.py <port>"
        sys.exit(1)
    __init_logging()

    port = int(sys.argv[1])
    ForkingServer(LolMatchData, port=port).start()
