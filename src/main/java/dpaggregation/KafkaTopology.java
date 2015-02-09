package dpaggregation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import common.CountStat;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentKafkaState;
import storm.kafka.trident.TridentKafkaStateFactory;
import storm.kafka.trident.TridentKafkaUpdater;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import aggregate.AggregateWindow;
import aggregate.StormAggregate;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

public class KafkaTopology {

	// Not the best way to share parameters with the aggregate window instances,
	// should probably pass a map of parameters to the factory...
	public static double bound;
	public static double epsilon;
	public static CountStat inputRate;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {

		if (args.length != 10) {
			System.out
					.println("Usage:\n\tKafkaTopology <kafkaZKHosts> <injectorTopic> <brokersList>"
							+ "<outputTopic> <Window Size (in seconds)> <window advance (in seconds)>"
							+ " <bound> <epsilon> <experimentDuration> <statsFile>");
			return;
		}

		String kafkaZKHosts = args[0];
		System.out.println("kafkaZKHosts: " + kafkaZKHosts);
		String injectorTopic = args[1];
		System.out.println("injectorTopic: " + injectorTopic);
		String brokersList = args[2];
		System.out.println("brokersList: " + brokersList);
		final String outputTopic = args[3];
		System.out.println("outputTopic: " + outputTopic);
		int winSize = Integer.valueOf(args[4]) * 1000;
		System.out.println("Window Size (in seconds): " + winSize);
		int winAdvance = Integer.valueOf(args[5]) * 1000;
		System.out.println("Window Advance (in seconds): " + winAdvance);
		KafkaTopology.bound = Double.valueOf(args[6]);
		System.out.println("Bound: " + KafkaTopology.bound);
		KafkaTopology.epsilon = Double.valueOf(args[7]);
		System.out.println("Epsilon: " + KafkaTopology.epsilon);
		long experimentDuration = Long.valueOf(args[8]) * 1000;
		System.out.println("experimentDuration: " + experimentDuration);
		String statsFile = args[9];
		System.out.println("statsFile: " + statsFile);

		inputRate = new CountStat("",statsFile,true);
		inputRate.start();

		TridentTopology topology = new TridentTopology();

		/*
		 * INJECTOR - READ TUPLE FROM KAFKA TOPIC
		 */

		BrokerHosts zk = new ZkHosts(kafkaZKHosts);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, injectorTopic);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		Stream stream = topology.newStream("input", spout).each(
				new Fields("str"), new Function() {

					private static final long serialVersionUID = 8406653966850084867L;

					@Override
					public void prepare(@SuppressWarnings("rawtypes") Map arg0,
							TridentOperationContext arg1) {

					}

					@Override
					public void cleanup() {

					}

					@Override
					public void execute(TridentTuple arg0, TridentCollector arg1) {
						inputRate.increase(1);
						String tuple = arg0.getStringByField("str");
						long sysTS = Long.valueOf(tuple.split(",")[0]);
						long ts = Long.valueOf(tuple.split(",")[1]);
						arg1.emit(Arrays.asList((Object) sysTS, (Object) ts,
								(Object) tuple.split(",")[2],
								(Object) Double.valueOf(tuple.split(",")[3])));
					}
				}, new Fields("sysTS", "ts", "house", "cons"));

		/*
		 * AGGREGATION
		 */

		class DPAgg implements AggregateWindow, Serializable {

			private double sum;
			private double count;
			private double boundedSum;
			private Random r;

			public AggregateWindow factory() {
				return new DPAgg();
			}

			public void setup() {
				sum = 0;
				count = 0;
				boundedSum = 0;
				r = new Random();
			}

			public void update(TridentTuple t) {
				double val = t.getDoubleByField("cons");
				double boundedVal = val < KafkaTopology.bound ? val
						: KafkaTopology.bound;
				sum += val;
				boundedSum += boundedVal;
				count++;
			}

			public List<Object> getAggregatedResult(long timestamp,
					String groupby) {
				double noise = laplace(KafkaTopology.bound
						/ KafkaTopology.epsilon);
				return Arrays.asList(timestamp, (Object) groupby, count, sum,
						boundedSum, boundedSum + noise);
			}

			private double laplace(double lambda) {
				double u = r.nextDouble() * -1 + 0.5;
				return -1
						* (lambda * Math.signum(u) * Math.log(1 - 2 * Math
								.abs(u)));
			}

			@Override
			public int getNumberOfOutFields() {
				return 7;
			}
		}

		Stream aggStream = stream.aggregate(
				new Fields("sysTS", "ts", "house", "cons"),
				new StormAggregate<DPAgg>("ts", "house", winSize, winAdvance,
						new DPAgg()),
				new Fields("sysTS", "ts", "house", "count", "sum",
						"boundedSum", "dpSum")).each(
				new Fields("sysTS", "ts", "house", "count", "sum",
						"boundedSum", "dpSum"), new Function() {

					@Override
					public void prepare(@SuppressWarnings("rawtypes") Map conf,
							TridentOperationContext context) {
					}

					@Override
					public void cleanup() {
					}

					@Override
					public void execute(TridentTuple tuple,
							TridentCollector collector) {
						collector.emit(Arrays.asList(
								(Object) outputTopic,
								tuple.toString()
										+ " - "
										+ (System.currentTimeMillis() - tuple
												.getLongByField("sysTS"))));
					}
				}, new Fields("key", "msg"));

		/*
		 * Push results to Kafka brokers
		 */

		@SuppressWarnings("rawtypes")
		TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
				.withKafkaTopicSelector(new DefaultTopicSelector(outputTopic))
				.withTridentTupleToKafkaMapper(
						new FieldNameBasedTupleToKafkaMapper("key", "msg"));
		aggStream.partitionPersist(stateFactory, new Fields("key", "msg"),
				new TridentKafkaUpdater(), new Fields());

		Config conf = new Config();
		conf.setDebug(false);
		Properties props = new Properties();
		props.put("metadata.broker.list", brokersList);
		// props.put("request.required.acks", "0");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);

		LocalCluster cluster = new LocalCluster();
		System.out.println("Submitting topology");
		cluster.submitTopology("dpds", conf, topology.build());
		backtype.storm.utils.Utils.sleep(experimentDuration);
		System.out.println("Killing topology");
		cluster.killTopology("dpds");
		cluster.shutdown();
		System.out.println("Cluster down...");

		System.out.println("Writing stats");
		inputRate.stopStats();
		inputRate.writeStats();

		System.exit(0);
	}
}
