

# League of Legends Demo

This Hadoop demo is an effort to produce a complete end-to-end pipeline using various Hadoop projects.

Currently implemented:
  * Pull League of Legend (LoL) match data as JSON, publishing to an rpyc server
  * Avro schema containing select fields from LoL data model
  * Kafka Producer, levering rpyc, receiving stream of LoL data
    * Converts LoL Data to Avro schema
    * Serialized Avro object to a binary representation
    * Publishes message to Kafka topic
  * Kafka Consumer
    * Receives messages from Kafka topic
    * Batches messages into a temporary file
    * Moves file to HDFS
  

Software Requirements
---------------------
The following softare products and other items are required.  Acquisition, Installation, configuration, and setup of these tools are outside the scope of this project.

* [HDFS](http://hadoop.apache.org)
* [Avro](http://avro.apache.org) tools jar file, Avro module (pip install avro)
* [Kafka](http://kafka.apache.org)
* [Redis](https://redis.io)
* [redispy](https://github.com/andymccurdy/redis-py) (pip install redis)
* [kafka-python](https://github.com/dpkp/kafka-python) __must be cloned and installed from Github.__ pip version is out of date
* [League of Legends API Key](https://developer.riotgames.com/), requires LoL account
* HDFS Client Machine (`hdfs` command line tool is available and properly configured)

##Python

###Part 1 - Avro Schema

Inspect `match.avsc`.  This Avro schema contains select fields from the [`match` API call](https://developer.riotgames.com/api/methods).

###Part 2 - LoL Ingestion

#### Overview
There are three files for ingesting data in Avro.

* __post_match.py__
  * Manages the REST calls to the LoL API
  * Stores metadata in Redis for searching for matches
  * Posts the `match` JSON object to a given rpyc server
* __producer.py__
  * Receives rpyc calls containing `match` JSON data
  * Pulls selected fields from `match` JSON data and creates an Avro objects
  * Encodes Avro object as binary data
  * Publishes binary Avro data to configured Kafka stream
* __consumer.py__
  * Receives messages from configured Kafka topic
  * Decodes binary Avro data to Avro object
  * Logs Avro object
  * Writes Avro object to local temporary file
  * After _n_ number of messages have been written, roll over file
  * Move file to configured HDFS directory
  
#### Execution


First, create the Kafka topic for posting messages, using your ZooKeeper and Kafka configuration

```
$ kafka-topics.sh --zookeeper localhost:2181 --create \
    --topic lol --partitions 1 --replication-factor 1
Created topic "lol".
```

Then, start the rpyc server/Kafka producer:

```
$ python producer.py 
usage: python producer.py <server.port> <brokers> <topic>
    server.port - port to bind to for rpyc calls
    brokers - comma-delimited list of host:port pairs for Kafka brokers
    topic - Kafka topic to post messages to, must exist
$ python producer.py 44444 localhost:9092 lol
2016-02-25 15:44:57,205 - kafka.client - INFO - Broker version identifed as 0.9
2016-02-25 15:44:57,208 - LOLMATCHDATA/44444 - INFO - server started on [0.0.0.0]:44444
```

In a separate terminal, start the Kafka consumer:

```
$ python consumer.py
usage: python consumer.py <brokers> <topic> <output> <messages.per.file>
    brokers - comma-delimited list of host:port pairs for Kafka brokers
    topic - Kafka topic to post messages to, must exist
    output - Absolute HDFS directory to write files to, e.g. /in/lol
    messages.per.file - Number of messages to write to a single file before rolling over
$ python consumer.py localhost:9092 lol /in/lol 10
```

In a third terminal, create a local file containing your API key:

```
$ cat api.key
<key omitted>
```

Start the `post_match` process to create the JSON stream.

```
$ python post_match.py 
usage: python post_match.py <rpyc.server> <api.key.file>
    rpyc.server - host:port for sending rpyc calls
    api.key.file - local file containing your LoL API key
$ python post_match.py localhost:44444 api.key 
```

Both the producer and consumer will log messages as they are produced/consumed.  After 10 messages are consumed, they will be written to a file in HDFS under the directory you specified.

```
$ hdfs dfs -ls -R /in/
drwxr-xr-x   - adamjshook supergroup          0 2016-02-25 15:59 /in/lol
drwxr-xr-x   - adamjshook supergroup          0 2016-02-25 15:59 /in/lol/2016
drwxr-xr-x   - adamjshook supergroup          0 2016-02-25 15:59 /in/lol/2016/02
drwxr-xr-x   - adamjshook supergroup          0 2016-02-25 16:00 /in/lol/2016/02/25
drwxr-xr-x   - adamjshook supergroup          0 2016-02-25 16:01 /in/lol/2016/02/25/21
-rw-r--r--   1 adamjshook supergroup       1329 2016-02-25 16:00 /in/lol/2016/02/25/21/data-1456452008.avro
-rw-r--r--   1 adamjshook supergroup       1329 2016-02-25 16:00 /in/lol/2016/02/25/21/data-1456452025.avro
-rw-r--r--   1 adamjshook supergroup       1329 2016-02-25 16:00 /in/lol/2016/02/25/21/data-1456452042.avro
-rw-r--r--   1 adamjshook supergroup       1329 2016-02-25 16:01 /in/lol/2016/02/25/21/data-1456452058.avro
-rw-r--r--   1 adamjshook supergroup       1329 2016-02-25 16:01 /in/lol/2016/02/25/21/data-1456452075.avro
-rw-r--r--   1 adamjshook supergroup       1329 2016-02-25 16:01 /in/lol/2016/02/25/21/data-1456452092.avro
```

You can download one of these files and view the contents using the Avro tools jar file.

```
$ hdfs dfs -get /in/lol/2016/02/25/21/data-1456452042.avro
$ java -jar /opt/avro/avro-tools-1.8.0.jar tojson data-1456452042.avro
{"lol.Match":{"mapId":1,"matchCreation":1366716629,"matchDuration":1638,"matchId":1366716629,"matchMode":"CLASSIC","winningTeam":100,"participants":[],"teams":[]}}
{"lol.Match":{"mapId":1,"matchCreation":1540888257,"matchDuration":1882,"matchId":1540888257,"matchMode":"CLASSIC","winningTeam":100,"participants":[],"teams":[]}}
...
```