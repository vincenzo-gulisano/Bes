package common;

import java.util.LinkedList;
import java.util.List;

import storm.trident.tuple.TridentTuple;

public class Utils {
	public static List<Object> getFlushTuple(int numFields) {
		LinkedList<Object> result = new LinkedList<Object>();
		result.add(TupleType.FLUSH);
		for (int i = 0; i < numFields; i++) {
			result.add(null);
		}
		return result;
	}

	public static boolean isFlushTuple(TridentTuple t) {
		if (((TupleType) t.getValue(0)) == TupleType.FLUSH)
			return true;
		return false;
	}
}
