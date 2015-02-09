package aggregate;

import java.util.List;

import storm.trident.tuple.TridentTuple;

/**
 * @author Vincenzo Gulisano
 * 
 * Interface for generic time-based sliding window
 *
 */
public interface AggregateWindow {

	/**
	 * @return a new AggregateWindow
	 */
	public AggregateWindow factory();

	/**
	 * Setup the window internal fields
	 */
	public void setup();
	
	/**
	 * Update the window with the contribution of the incoming tuple t
	 * @param t The incoming tuple updating the window
	 */
	public void update(TridentTuple t);

	/**
	 * @param timestamp The timestamp of the output tuple 
	 * @param groupby The Group-By of the output tuple
	 * @return The output tuple (as list of object)
	 */
	public List<Object> getAggregatedResult(long timestamp, String groupby);

	/**
	 * @return the number of fields of the output tuple
	 */
	public int getNumberOfOutFields();
	
}
