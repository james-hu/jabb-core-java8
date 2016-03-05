/**
 * 
 */
package net.sf.jabb.dstream;

import java.util.function.Function;

import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;

/**
 * Data structure for a StreamDataSupplier, an ID, a from position, and a to position.
 * The the from and to positions can be inclusive or exclusive, depending on the upper level application.
 * @author James Hu
 * 
 * @param <M> type of the message object
 *
 */
public class StreamDataSupplierWithIdAndPositionRange<M> extends SimpleStreamDataSupplierWithId<M> implements StreamDataSupplierWithIdAndRange<M, String>{
	protected String fromPosition;
	protected String toPosition;
	
	public StreamDataSupplierWithIdAndPositionRange(){
		super();
	}
	
	public StreamDataSupplierWithIdAndPositionRange(String id, StreamDataSupplier<M> supplier){
		super(id, supplier);
	}
	
	public StreamDataSupplierWithIdAndPositionRange(String id, StreamDataSupplier<M> supplier, String fromPosition, String toPosition){
		super(id, supplier);
		this.fromPosition = fromPosition;
		this.toPosition = toPosition;
	}
	
	@Override
	public ReceiveStatus receiveInRange(Function<M, Long> receiver, String startPosition, String endPosition) throws DataStreamInfrastructureException {
		if (startPosition == null || startPosition.length() == 0 || supplier.isInRange(startPosition, fromPosition)){
			startPosition = fromPosition;
		}
		if (endPosition == null || endPosition.length() == 0 || supplier.isInRange(toPosition, endPosition)){
			endPosition = toPosition;
		}
		return supplier.receive(receiver, startPosition, endPosition);
	}

	@Override
	public String getFrom(){
		return fromPosition;
	}

	@Override
	public String getTo(){
		return toPosition;
	}


	/**
	 * Get the from position
	 * @return	the from position, can be null
	 */
	public String getFromPosition() {
		return fromPosition;
	}
	
	/**
	 * Set the from position
	 * @param fromPosition		the from position, can be null
	 */
	public void setFromPosition(String fromPosition) {
		this.fromPosition = fromPosition;
	}
	
	/**
	 * Get the to position
	 * @return	the to position, can be null
	 */
	public String getToPosition() {
		return toPosition;
	}
	
	/**
	 * Set the to position
	 * @param toPosition	the to position, can be null
	 */
	public void setToPosition(String toPosition) {
		this.toPosition = toPosition;
	}

}
