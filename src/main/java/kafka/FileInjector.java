package kafka;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import common.CountStat;
import common.NativeSleep;

public abstract class FileInjector implements Runnable {

//	private long referenceNanoSecond;
	private int batchSize;
	private long currentDecreaseWait;
	private int periodIndex;
	protected Random random;
	private List<Long> periods;
	private List<Long> waits;
	private boolean sleepOnlyForBatches;
	private CountStat inputRate;
	Producer<String, String> producer;

	public FileInjector(/*long referenceNanoSecond, */int batchSize,
			String periodsFile, boolean sleepOnlyForBatches,
			ProducerConfig config,String statsFile) {

		this.producer = new Producer<String, String>(config);
		this.inputRate = new CountStat("",statsFile,true);
		this.inputRate.start();
		this.sleepOnlyForBatches = sleepOnlyForBatches;
//		this.referenceNanoSecond = referenceNanoSecond;
		this.batchSize = batchSize;
		this.periodIndex = 0;
		this.currentDecreaseWait = System.currentTimeMillis() / 1000;
		this.periods = new LinkedList<Long>();
		this.waits = new LinkedList<Long>();

		try {
			BufferedReader br = new BufferedReader(new FileReader(periodsFile));
			String line;
			while ((line = br.readLine()) != null) {

				double thisInputRate = Double.valueOf(line.split(",")[0]);

				if (this.sleepOnlyForBatches) {
					thisInputRate /= this.batchSize;
				}
				double thisSleepPeriod = 1000000 / thisInputRate;

				this.periods.add((long) Math.floor(thisSleepPeriod));

				this.waits.add(Long.valueOf(line.split(",")[1]));
				System.out.println("Adding this sleep period ("
						+ ((long) Math.floor(thisSleepPeriod)) + ")");
				System.out.println("Added rate " + thisInputRate);

			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	protected abstract void setup();

	protected abstract boolean hasMoreTuples();

	protected abstract KeyedMessage<String, String> getNextTuple();

	protected abstract void close();

	@Override
	public void run() {

		setup();

		currentDecreaseWait = System.currentTimeMillis() / 1000;

		List<KeyedMessage<String, String>> batch = new LinkedList<KeyedMessage<String, String>>();

		long accumulated_delay = 0;

		boolean done = false;
		boolean sleepThisRound = false;

		long before_ts = System.nanoTime();

		System.out.println("Generator starting at "
				+ System.currentTimeMillis());
		while (!done && hasMoreTuples()) {

			sleepThisRound = false;

			KeyedMessage<String, String> t = getNextTuple();
			// KeyedMessage<String, String> t = getNextTuple(
			// (System.nanoTime() - referenceNanoSecond) / 1000,
			// System.nanoTime());
			// tupleCounter++;

			batch.add(t);
			if (batch.size() == batchSize) {
				// System.out.println("Sending batch " + batch);
				producer.send(batch);
				this.inputRate.increase(batchSize);
				batch = new LinkedList<KeyedMessage<String, String>>();
				sleepThisRound = true;
			}

			if (!sleepOnlyForBatches || sleepThisRound) {

				long after_ts = System.nanoTime();
				long sleep_us = this.periods.get(periodIndex).longValue()
						- (after_ts - before_ts) / 1000 - accumulated_delay;

				if (sleep_us > 0) {
					long a = System.nanoTime();
					NativeSleep.sleep((int) sleep_us);
					long b = System.nanoTime();
					accumulated_delay = ((b - a) / 1000 - sleep_us);
				} else {
					accumulated_delay -= this.periods.get(periodIndex)
							.longValue();
				}

				before_ts = System.nanoTime();

			}

			if (System.currentTimeMillis() / 1000 - currentDecreaseWait > this.waits
					.get(periodIndex)) {
				periodIndex++;
				if (periodIndex == this.periods.size()) {
					done = true;
				} else {
					currentDecreaseWait = System.currentTimeMillis() / 1000;
				}
			}

		}

		if (batch.size() > 0) {
			System.out.println("Sending last batch");
			producer.send(batch);
			this.inputRate.increase(batchSize);
			batch = new LinkedList<KeyedMessage<String, String>>();
		}

		System.out.println("Injector, all tuples sent!");
		producer.close();

	}

	public void writeStats() {

		this.inputRate.stopStats();
		this.inputRate.writeStats();

	}

}
