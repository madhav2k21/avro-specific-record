package com.techleads.app.runner;

import java.io.File;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.tecleads.app.avro.Customer;

@Component
public class SpecificRecordTest implements CommandLineRunner {

	@Override
	public void run(String... args) throws Exception {
		// step0: crate a Specific record
		Customer.Builder customerBuilder = Customer.newBuilder();
		customerBuilder.setFirstName("Madhav");
		customerBuilder.setLastName("Anupoju");
		customerBuilder.setAge(25);
		customerBuilder.setHeight(5.3f);
		customerBuilder.setWeight(55.5f);
		customerBuilder.setAutomatedEmail(false);
		Customer customer = customerBuilder.build();
		System.out.println(customer);

		// step2: write that generic record to a file
		// writing to a file
		final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
		try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
			dataFileWriter.append(customer);
			System.out.println("*****Written successfully customer-specific.avro****");

		} catch (Exception e) {
			System.out.println("Exception while writing avro " + e.getMessage());
		}

		// step3: read a generic record from a file
		final File file = new File("customer-specific.avro");
		final DatumReader<Customer> datumReader = new SpecificDatumReader<>();
		DataFileReader<Customer> dataFileReader;
		try {
			System.out.println("Reading our specific record");
			dataFileReader = new DataFileReader<>(file, datumReader);
			while (dataFileReader.hasNext()) {
				Customer readCustomer = dataFileReader.next();
				System.out.println(readCustomer.toString());
				System.out.println("FirstName: " + readCustomer.getFirstName());
				System.out.println("Last Name: " + readCustomer.getLastName());
				System.out.println("Age: " + readCustomer.getAge());
				System.out.println("Height: " + readCustomer.getHeight());
				System.out.println("Weight: " + readCustomer.getWeight());
				System.out.println("Automated Email: " + readCustomer.getAutomatedEmail());
			}

		} catch (Exception e) {
			System.out.println("exception while writing to a file " + e.getMessage());
		}
		System.exit(0);

	}

}