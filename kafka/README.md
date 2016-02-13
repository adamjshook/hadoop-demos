

# Kafka Demo
This is some code taken from [the Apache Kafka quickstart docs](https://kafka.apache.org/documentation.html#quickstart) for basic examples of working with Kafka in Java and Python.


----------

Java
-----

View `kafka/src/main/java/com/adamjshook/demo/kafka/Driver.java` using your preferred Java IDE or a text editor.  It contains the example code found in the Kafka Javadocs.

Build the code using Maven

`$ mvn clean package`

Create a kafka topic to use for the demo.  This assumes Kafka has already been installed and running on the same host.  See the quickstart guide for details.

`$ kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --replication-factor 1 --topic my-java-test`

Start the consumer in a terminal window, passing in the broker list, topic name, and `consumer`.

`$ java -jar target/kafka-1.0.0.jar localhost:9092 my-java-test consumer`

In another terminal, invoke the following command to start the producer.  Same arguments as before, but use `producer` instead of `consumer`.

`$ java -jar target/kafka-1.0.0.jar localhost:9092 my-java-test producer`

This program will complete.  Return to your consumer tab, and view the output.

Python
--------

The version in `pip` for `kafka-python` is not yet up to date, so clone the project and install it by hand.

```bash
$ git clone https://github.com/dpkp/kafka-python.git
$ cd kafka-python/
$ sudo python setup.py install
```

Create a kafka topic to use for the demo.  Again, this assumes Kafka has already been installed and running on the same host.  See the quickstart guide for details.

`$ kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --replication-factor 1 --topic my-python-test`

Start the consumer in a terminal window, passing in the broker list, topic name, and `consumer`.

`$ python consumer.py localhost:9092 my-python-test`

In another terminal, invoke the following command to start the producer.  Same arguments as before, but use `producer.py` instead of `consumer.py`.

`$ python producer.py localhost:9092 my-python-test`

This program will complete.  Return to your consumer tab, and view the output.