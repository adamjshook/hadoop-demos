import rpyc
from rpyc.utils.server import ForkingServer
import sys
import json
import logging

def __init_logging():
    root = logging.getLogger()
    root.setLevel(logging.getLevelName("INFO"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(ch)

class LolMatchData(rpyc.Service):
    def exposed_match(self, dataStr):
        data = json.loads(dataStr)
        print json.dumps(data, indent=4, separators=(',', ': '))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "usage: python producer.py <port>"
        sys.exit(1)
    __init_logging()

    port = int(sys.argv[1])
    ForkingServer(LolMatchData, port=port).start()

