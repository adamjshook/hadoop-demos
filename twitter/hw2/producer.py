#!/usr/bin/python

import avro.schema, json, logging, sys, time
from avro.io import AvroTypeException, BinaryEncoder, DatumWriter
from kafka import KafkaProducer
from StringIO import StringIO

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Parse the Avro schema and create a DatumWriter
schema = avro.schema.parse(open("tweet.avsc").read())
writer = DatumWriter(writers_schema=schema)

topic = None  # The topic to post messages to
producer = None  # The Kafka producer

# ADD YOUR STUFF HERE!
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

#count = 0
class MyStreamListener(StreamListener):
    """ 
    A listener handles tweets that are received from the stream.
    """
    def on_data(self, dataStr):
        data = json.loads(dataStr)

        if( "created_at" not in data ):
            #pass
            return True

        avro_obj = dict()
        avro_obj["user_id"] = data["user"]["id"]
        avro_obj["screen_name"] = data["user"]["screen_name"]
        avro_obj["location"] =  data["user"]["location"]
        avro_obj["description"] =  data["user"]["description"]
        avro_obj["followers_count"] = data["user"]["followers_count"]
        avro_obj["statuses_count"] = data["user"]["statuses_count"]
        avro_obj["geo_enabled"] = data["user"]["geo_enabled"]
        avro_obj["lang"] = data["user"]["lang"]

        if "coordinates" in data and data["coordinates"] != None:
                avro_obj["longitude"] = data["coordinates"]["coordinates"][0]
                avro_obj["latitude"] = data["coordinates"]["coordinates"][1]

        avro_hashtags = []
        avro_url = []
        avro_mentions = []

        for msg in data["entities"]["hashtags"]:
            avro_hashtags.append(msg["text"])

        for msg in data["entities"]["urls"]:
            avro_url.append(msg["display_url"])

        for msg in data["entities"]["user_mentions"]:
            avro_mentions.append(msg["id"])

        avro_obj["created_at"] = data["created_at"]
        avro_obj["id"] = data["id"]
        avro_obj["text"] = data["text"]
        avro_obj["source"] = data["source"]
        avro_obj["retweet_count"] = data["retweet_count"]
        avro_obj["favorite_count"] = data["favorite_count"]

        avro_obj["hashtags"] = avro_hashtags
        avro_obj["urls"] = avro_hashtags
        avro_obj["mentions"] = avro_hashtags

        print(json.dumps(avro_obj))
 
        # Create a string stream and encoder
        stream = StringIO()
        encoder = BinaryEncoder(stream)

        # Encode the avro object
        writer.write(avro_obj, encoder)

        # Send the encoded data to the producer and flush the message
        producer.send(topic, stream.getvalue())
        producer.flush()
        return True

    def on_error(self, status):
        print(status)


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print "usage: python producer.py <brokers> <topic>"
        print "    brokers - comma-delimited list of host:port pairs for Kafka brokers"
        print "    topic - Kafka topic to post messages to, must exist"
        sys.exit(1)

    brokers = sys.argv[1]
    topic = sys.argv[2]
    
    producer = KafkaProducer(bootstrap_servers=brokers)
    topic = topic

    l = MyStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['umbc', 'class', 'college', 'university'])

