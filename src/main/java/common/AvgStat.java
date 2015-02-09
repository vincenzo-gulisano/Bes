package common;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author Vincenzo Gulisano
 * 
 *         Class to maintain average statistics (measured every second)
 *
 */
public class AvgStat extends Thread implements Serializable {

	private static final long serialVersionUID = 2415520958777993198L;

	private long sum;
	private long count;
	private TreeMap<Long, Long> stats;

	private boolean stop;
	private long sleepms;

	String id;

	PrintWriter out;
	boolean immediateWrite;

	public AvgStat(String id, String outputFile, boolean immediateWrite) {
		this.id = id;
		this.sum = 0;
		this.count = 0;
		this.stats = new TreeMap<Long, Long>();
		this.stop = false;
		this.sleepms = 1000;
		this.immediateWrite = immediateWrite;

		FileWriter outFile;
		try {
			outFile = new FileWriter(outputFile);
			out = new PrintWriter(outFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void add(long v) {
		sum += v;
		count++;
	}

	@Override
	public void run() {
		while (!stop) {

			long start = System.currentTimeMillis();
			long time = System.currentTimeMillis();
			if (immediateWrite) {
				out.println(time + "," + (count != 0 ? sum / count : 0));
				out.flush();
			} else {
				this.stats.put(time, count != 0 ? sum / count : 0);
			}

			sum = 0;
			count = 0;

			long stop = System.currentTimeMillis();
			if ((sleepms - (stop - start)) > 0)
				try {
					Thread.sleep((sleepms - (stop - start)));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

		}
	}

	public void stopStats() {
		this.stop = true;
	}

	public void writeStats() {

		if (!immediateWrite) {
			try {
				for (Entry<Long, Long> stat : stats.entrySet()) {

					long time = stat.getKey();
					long counter = stat.getValue();

					out.println(time + "," + counter);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		out.flush();
		out.close();

	}

}
