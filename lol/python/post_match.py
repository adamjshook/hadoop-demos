import urllib2
import json
import redis
import rpyc
import sys
import logging
from time import sleep

r = redis.StrictRedis(host='localhost', port=6379, db=0)

FEATURED_GAMES = "https://na.api.pvp.net/observer-mode/rest/featured"
SUMMONER_BY_NAME = "https://na.api.pvp.net/api/lol/na/v1.4/summoner/by-name"
MATCH_LIST = "https://na.api.pvp.net/api/lol/na/v2.2/matchlist/by-summoner"
MATCH = "https://na.api.pvp.net/api/lol/na/v2.2/match"

NEW_SUMMONER_NAMES = "new_summoner_names"
ALL_SUMMONER_IDS = "all_summoner_ids"
SUMMONER_NAME_TO_ID = "summoner_name_to_id"
NEW_MATCH_IDS = "new_match_ids"
ALL_MATCH_IDS = "all_match_ids"

def __get_summoners(apiKey):
    url = "%s?api_key=%s" % (FEATURED_GAMES, apiKey)
    data = json.loads(urllib2.urlopen(url).read())
    gameList = data['gameList']
    names = []
    for game in gameList:
        for p in game['participants']:
            names.append(p['summonerName'])

    return names

def __get_id(name, apiKey):
    url = "%s/%s?api_key=%s" % (SUMMONER_BY_NAME, urllib2.quote(name), apiKey)
    data = json.loads(urllib2.urlopen(url).read())
    return data[name.lower().replace(' ','')]["id"]

def __get_new_summoner_ids(apiKey):
    if r.scard(NEW_SUMMONER_NAMES) == 0:
        for name in __get_summoners(apiKey):
            if not r.exists(SUMMONER_NAME_TO_ID) or not r.hexists(SUMMONER_NAME_TO_ID, name):
                r.sadd(NEW_SUMMONER_NAMES, name)

    if r.scard(NEW_SUMMONER_NAMES) > 0:
        name = r.spop(NEW_SUMMONER_NAMES)
        id = __get_id(name, apiKey)
        r.hset(SUMMONER_NAME_TO_ID, name, id)
        r.sadd(ALL_SUMMONER_IDS, id)

def __get_new_matches(id, apiKey):
    url = "%s/%s?api_key=%s" % (MATCH_LIST, id, apiKey)
    data = json.loads(urllib2.urlopen(url).read())
    if "matches" in data:
        for m in data["matches"]:
            matchId = m["matchId"]
            if not r.exists(ALL_MATCH_IDS) or not r.sismember(ALL_MATCH_IDS, matchId):
                r.sadd(ALL_MATCH_IDS, matchId)
                r.sadd(NEW_MATCH_IDS, matchId)

def __get_match(matchId, apiKey):
    url = "%s/%s?api_key=%s" % (MATCH, matchId, apiKey)
    return urllib2.urlopen(url).read()

def __get_new_match_data(apiKey):
    if not r.exists(NEW_MATCH_IDS) or r.scard(NEW_MATCH_IDS) == 0:
        __get_new_summoner_ids(apiKey)
        randId = r.srandmember(ALL_SUMMONER_IDS)
        __get_new_matches(randId, apiKey)

    if r.scard(NEW_MATCH_IDS) > 0:
        randMatch = r.spop(NEW_MATCH_IDS)
        return __get_match(randMatch, apiKey)
    else:
        return None

def __connect(address):
    '''A function to connect to an rpyc server at the given address'''
    hostname, port = __split_hostport(address)

    try:
        a = rpyc.connect(str(hostname), int(port))
        return a
    except Exception as e:
        logging.warning(' '.join(['There was a problem connecting to', hostname, str(port)]))
        raise e


def __split_hostport(hostnameport):
    '''A convenience function that takes a string "x.x.x.x:yyyy' and splits it into "x.x.x.x", yyyy'''
    h, p = hostnameport.split(':')
    return h, int(p)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: python post_match.py <rpyc.server> <api.key.file>"
        print "    rpyc.server - host:port for sending rpyc calls"
        print "    api.key.file - local file containing your LoL API key"
        sys.exit(1)

    server = sys.argv[1]
    apiKey = open(sys.argv[2], 'r').read()

    c = __connect(server)
    while True:
        try:
            c.root.match(__get_new_match_data(apiKey))
            sleep(1)
        except urllib2.HTTPError as e:
            # Guessing this is a rate limit thing
            print e
            sleep(10)
        except KeyError as e:
            # Occurs when some stuff is Unicode
            print e
