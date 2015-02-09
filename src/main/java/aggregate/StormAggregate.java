package aggregate;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import common.TupleType;
import common.Utils;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class StormAggregate<T extends AggregateWindow> implements
		Aggregator<Aggregate<? extends AggregateWindow>> {

	private static final long serialVersionUID = 8576660699312878285L;

	Aggregate<? extends AggregateWindow> aggregate;

	private String timestampFieldID;
	private String groupbyFieldID;
	private long windowSize;
	private long windowAdvance;
	private T aggregateWindow;
	private boolean setTupleType = false;

	public StormAggregate(String timestampFieldID, String groupbyFieldID,
			long windowSize, long windowAdvance, T aggregateWindow) {
		this.timestampFieldID = timestampFieldID;
		this.groupbyFieldID = groupbyFieldID;
		this.windowSize = windowSize;
		this.windowAdvance = windowAdvance;
		this.aggregateWindow = aggregateWindow;
	}

	public void setTupleType(boolean tupleType) {
		this.setTupleType = tupleType;
	}

	public void cleanup() {

	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TridentOperationContext arg1) {
		aggregate = new Aggregate<AggregateWindow>(timestampFieldID,
				groupbyFieldID, windowSize, windowAdvance, aggregateWindow);
	}

	public void aggregate(Aggregate<? extends AggregateWindow> agg,
			TridentTuple t, TridentCollector collector) {

		if (setTupleType && Utils.isFlushTuple(t)) {
			collector.emit(Utils.getFlushTuple(aggregateWindow
					.getNumberOfOutFields()));
		} else {
			List<List<Object>> outputs = agg.processTuple(t);
			for (List<Object> out : outputs) {

				// Add tuple type
				LinkedList<Object> temp = new LinkedList<Object>();
				if (setTupleType)
					temp.add(TupleType.REGULAR);

				if (t.contains("sysTS"))
					temp.add(t.getLongByField("sysTS"));

				temp.addAll(out);
				collector.emit(temp);
			}
		}
	}

	public void complete(Aggregate<? extends AggregateWindow> arg0,
			TridentCollector arg1) {

	}

	public Aggregate<? extends AggregateWindow> init(Object arg0,
			TridentCollector arg1) {
		return aggregate;
	}

}
