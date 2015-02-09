package dpaggregation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.FileInjector;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaInjector {
	public static void main(String[] args) {

		if (args.length != 6) {
			System.out
					.println("Usage:\nKafkaInjector inputFile periodsFile statsFile batchSize "
							+ "sleepOnlyForBatches brokerList");
			return;
		}

		final String inputFile = args[0];
		System.out.println("inputFile: " + inputFile);
		String periodsFile = args[1];
		System.out.println("periodsFile: " + periodsFile);
		String statsFile = args[2];
		System.out.println("statsFile: " + statsFile);
		int batchSize = Integer.valueOf(args[3]);
		System.out.println("batchSize: " + batchSize);
		boolean sleepOnlyForBatches = Boolean.valueOf(args[4]);
		System.out.println("sleepOnlyForBatches: " + sleepOnlyForBatches);
		String brokerList = args[5];
		System.out.println("brokerList: " + brokerList);

		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "0");

		ProducerConfig config = new ProducerConfig(props);

		FileInjector injector = new FileInjector(batchSize, periodsFile,
				sleepOnlyForBatches, config,statsFile) {

			private BufferedReader br;
			private String nextLine;

			@Override
			protected KeyedMessage<String, String> getNextTuple() {
				// The string message will look like this:
				// systemInputTS,1136073600000,202004,2127
				return new KeyedMessage<String, String>("input",
						System.currentTimeMillis() + "," + nextLine);
			}

			@Override
			protected boolean hasMoreTuples() {
				try {
					nextLine = br.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return nextLine != null;
			}

			@Override
			protected void setup() {
				nextLine = "";
				try {
					br = new BufferedReader(new FileReader(inputFile));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}

			@Override
			protected void close() {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		};
		Thread t = new Thread(injector);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		injector.writeStats();

	}
}