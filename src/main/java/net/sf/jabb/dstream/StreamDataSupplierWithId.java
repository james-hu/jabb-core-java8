/**
 * Created by mjohnson on 2/29/2016.
 */
package net.sf.jabb.dstream;

import java.time.Instant;

/**
 * Data structure for a StreamDataSupplier and an ID.
 *
 * @param <M>	type of the message
 */
public interface StreamDataSupplierWithId<M> {
	StreamDataSupplierWithIdAndPositionRange<M> withRange(String fromPosition, String toPosition);
	
	StreamDataSupplierWithIdAndEnqueuedTimeRange<M> withRange(Instant fromTime, Instant toTime);
	
	/**
	 * Get the ID of the stream. Useful for logging
	 * @return	the ID
	 */
	String getId();
	
	/**
	 * Get the stream data supplier
	 * @return	the stream data supplier
	 */
	StreamDataSupplier<M> getSupplier();
	
}
