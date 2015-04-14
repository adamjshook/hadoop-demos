package com.adamjshook.demo.avro;

import java.io.File;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import example.avro.User;

public class Driver {
	public static void main(String[] args) throws Exception {
		demoTypedObjects();
		demoGenericObjects();
	}

	private static void demoTypedObjects() throws Exception {
		System.out.println("-- Using Users.java objects --\n");

		// Let's create some User objects
		User user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null

		// Alternate constructor
		User user2 = new User("Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder().setName("Charlie")
				.setFavoriteColor("blue").setFavoriteNumber(null).build();

		File file = new File("users.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);

		dataFileWriter.create(user1.getSchema(), new File("users.avro"));

		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.close();

		// Deserialize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(
				User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file,
				userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
		dataFileReader.close();
	}

	private static void demoGenericObjects() throws Exception {
		System.out.println("-- Using GenericRecords from user.avsc --\n");

		Schema schema = new Schema.Parser().parse(new File("../user.avsc"));
		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Alyssa");
		user1.put("favorite_number", 256);
		// Leave favorite color null

		try {
			user1.put("favorite_animal", "cat");
		} catch (AvroRuntimeException e) {
			System.err.println("Caught a AvroRuntimeException: "
					+ e.getMessage());
		}

		GenericRecord user2 = new GenericData.Record(schema);
		user2.put("name", "Ben");
		user2.put("favorite_number", 7);
		user2.put("favorite_color", "red");

		// Serialize user1 and user2 to disk
		File file = new File("users.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.close();

		// // Deserialize users from disk
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}

		dataFileReader.close();
	}
}