package com.adamjshook.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class Driver
{
	public static void main(String[] args)
			throws Exception
	{
		if (args.length != 3) {
			System.err.println("Usage: java -jar <jarfile> <brokers> <topic> <producer|consumer>");
			System.exit(1);
		}

		String servers = args[0];
		String topic = args[1];
		String type = args[2];

		Properties props = new Properties();
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("bootstrap.servers", servers);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		if (type.equals("producer")) {
			produceMessages(props, topic);
		}
		else if (type.equals("consumer")) {
			consumeMessages(props, topic);
		}
		else {
			System.err.println("<type> must be producer or consumer");
			System.exit(1);
		}

		System.exit(0);
	}

	private static void produceMessages(Properties props, String topic)
	{
		System.out.print("Producing messages...");
		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
		}
		producer.close();
		System.out.println(" Done.");
	}

	private static void consumeMessages(Properties props, String topic)
	{
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		System.out.println("Consumer started... Ctrl+C to quit");
		boolean consume = true;
		while (consume) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
			}
		}
		consumer.close();
	}
}