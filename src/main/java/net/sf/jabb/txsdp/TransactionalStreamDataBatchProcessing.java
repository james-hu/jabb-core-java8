/**
 * Created by mjohnson on 2/29/2016.
 */
package net.sf.jabb.txsdp;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedMap;

import net.sf.jabb.dstream.StreamDataSupplierWithIdAndRange;
import net.sf.jabb.dstream.ex.DataStreamInfrastructureException;
import net.sf.jabb.seqtx.SequentialTransactionsCoordinator.TransactionCounts;
import net.sf.jabb.seqtx.ex.TransactionStorageInfrastructureException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public interface TransactionalStreamDataBatchProcessing<M> {
	
	/**
	 * Sate of the processing and/or of the processor
	 * @author James Hu
	 *
	 */
	public static enum State{
		READY, 		// just initialized
		STOPPED, 	// stopped, run() method exited
		PAUSED, 	// paused, run() method is still executing, waiting for the processing to be resumed
		RUNNING, 	// running
		FINISHED, 	// all the data within the range had been processed, run() method exited
		STOPPING, 	// will be stopped soon, run() method is still executing
		PAUSING;	// will be paused soon, run() method is still executing
		
		public boolean isUnused(){
			return FINISHED.equals(this) || STOPPED.equals(this) || READY.equals(this);
		}
	}

	/**
	 * Processing status for a single stream with range.
	 * <ul>
	 * 	<li>finishedPosition - end position of the last transaction that is finished and all its successors are finished</li>
	 * 	<li>finishedEnqueuedTime - enqueued time of the message at finishedPosition</li>
	 * 	<li>lastUnfinishedStartPosition - start position of the last in progress transaction</li>
	 * 	<li>lastUnfinishedStartEnqueuedTime - enqueued time of the message at lastInProgressStartPosition</li>
	 * 	<li>lastUnfinishedEndPosition - end position of the last in progress transaction</li>
	 * 	<li>lastUnfinishedEndEnqueuedTime - enqueued time of the message at lastInProgressEndPosition</li>
	 * </ul>
	 * 
	 * @author James Hu
	 *
	 */
	public static class StreamStatus{
		protected String finishedPosition;
		protected Instant finishedEnqueuedTime;
		protected String lastUnfinishedStartPosition;
		protected Instant lastUnfinishedStartEnqueuedTime;
		protected String lastUnfinishedEndPosition;
		protected Instant lastUnfinishedEndEnqueuedTime;
		protected TransactionCounts transactionCounts;
		
		public StreamStatus(){
		}
		
		public StreamStatus(String finishedPosition, Instant finishedEnqueuedTime, String lastUnfinishedStartPosition,
				Instant lastUnfinishedStartEnqueuedTime, String lastUnfinishedEndPosition, Instant lastUnfinishedEndEnqueuedTime,
				TransactionCounts transactionCounts) {
			this.finishedPosition = finishedPosition;
			this.finishedEnqueuedTime = finishedEnqueuedTime;
			this.lastUnfinishedStartPosition = lastUnfinishedStartPosition;
			this.lastUnfinishedStartEnqueuedTime = lastUnfinishedStartEnqueuedTime;
			this.lastUnfinishedEndPosition = lastUnfinishedEndPosition;
			this.lastUnfinishedEndEnqueuedTime = lastUnfinishedEndEnqueuedTime;
			this.transactionCounts = transactionCounts;
		}
		
		public StreamStatus(String finishedPosition, Instant finishedEnqueuedTime, String lastUnfinishedStartPosition,
				Instant lastUnfinishedStartEnqueuedTime, String lastUnfinishedEndPosition, Instant lastUnfinishedEndEnqueuedTime,
				int inProgress, int retrying, int failed) {
			this(finishedPosition, finishedEnqueuedTime, lastUnfinishedStartPosition,
				lastUnfinishedStartEnqueuedTime, lastUnfinishedEndPosition, lastUnfinishedEndEnqueuedTime,
				new TransactionCounts(inProgress, retrying, failed));
		}
		


		@Override
		public String toString(){
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	
		/**
		 * @return the finishedPosition
		 */
		public String getFinishedPosition() {
			return finishedPosition;
		}
	
		/**
		 * @return the finishedEnqueuedTime
		 */
		public Instant getFinishedEnqueuedTime() {
			return finishedEnqueuedTime;
		}
	
		/**
		 * @return the lastUnfinishedStartPosition
		 */
		public String getLastUnfinishedStartPosition() {
			return lastUnfinishedStartPosition;
		}
	
		/**
		 * @return the lastUnfinishedStartEnqueuedTime
		 */
		public Instant getLastUnfinishedStartEnqueuedTime() {
			return lastUnfinishedStartEnqueuedTime;
		}
	
		/**
		 * @return the lastUnfinishedEndPosition
		 */
		public String getLastUnfinishedEndPosition() {
			return lastUnfinishedEndPosition;
		}
	
		/**
		 * @return the lastUnfinishedEndEnqueuedTime
		 */
		public Instant getLastUnfinishedEndEnqueuedTime() {
			return lastUnfinishedEndEnqueuedTime;
		}
	
		/**
		 * @return the transactionCounts
		 */
		public TransactionCounts getTransactionCounts() {
			return transactionCounts;
		}
	}

	/**
	 * Status of a single processor
	 * @author James Hu
	 *
	 */
	public static class ProcessorStatus{
		protected State state;
	
		public ProcessorStatus(){
			
		}
		
		public ProcessorStatus(State state){
			this.state = state;
		}
		
		@Override
		public String toString(){
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
		
		/**
		 * @return the state
		 */
		public State getState() {
			return state;
		}
	}

	/**
	 * Overall status of the processing
	 * @author James Hu
	 *
	 */
	public static class Status{
		protected SortedMap<String, ProcessorStatus> processorStatus;
		protected LinkedHashMap<String, StreamStatus> streamStatus;
		
		public Status(){
			
		}
		
		public Status(SortedMap<String, ProcessorStatus> processorStatus, LinkedHashMap<String, StreamStatus> streamStatus){
			this.processorStatus = processorStatus;
			this.streamStatus = streamStatus;
		}
		
		@Override
		public String toString(){
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
		
		/**
		 * @return the processorStatus, ordered by the ids of the processors
		 */
		public SortedMap<String, ProcessorStatus> getProcessorStatus() {
			return processorStatus;
		}
		/**
		 * @return the streamStatus, in the same order as the streams appears in the input to the TransactionalStreamDataBatchProcessing
		 */
		public LinkedHashMap<String, StreamStatus> getStreamStatus() {
			return streamStatus;
		}
	}

	/**
	 * Set or change the suppliers. Changing suppliers while processors are running is allowed.
	 * Processors are able to detect the change and start working with the new suppliers.
	 * @param suppliers the suppliers to set
	 */
	void setSuppliers(List<StreamDataSupplierWithIdAndRange<M, ?>> suppliers);

	/**
	 * Create a processor that does the processing.
	 * The processor created will be registered in the processing until removed.
	 * The newly created processor will be in READY state. You may want to start it after calling this method.
	 * @param processorId	ID of the processor
	 * @return	a runnable processor that can be run in any thread
	 */
	Runnable createProcessor(String processorId);
	
	/**
	 * Notify the processor to start processing. Once started, the processing can later be paused or stopped.
	 * @param processorId ID of the processor to be started
	 */
	void start(String processorId);
	
	/**
	 * Notify the processor to pause processing. Once paused, the processing can later be started or stopped.
	 * @param processorId ID of the processor to be paused
	 */
	void pause(String processorId);
	
	/**
	 * Notify the processor to stop processing. Once stopped, the processing cannot be restarted.
	 * @param processorId ID of the processor to be stopped
	 */
	void stop(String processorId);
	
	/**
	 * Remove a processor. After calling this method, the processor will not be controllable or visible through TransactionalStreamDataBatchProcessing.
	 * This method does not check the state of the processor. Normally you need to make sure that the processor has already been stopped.
	 * @param processorId ID of the processor to be stopped
	 */
	void remove(String processorId);
	
	/**
	 * Remove processors that are not in use. After calling this method, those processors will not be controllable or visible through TransactionalStreamDataBatchProcessing.
	 * All processors currently in READY or STOPPED or FINISHED states will be removed
	 */
	void removeUnused();
	
	/**
	 * Start all processors
	 */
	void startAll();
	
	/**
	 * Notify all processors to pause. Paused processors can be restarted or stopped later.
	 */
	void pauseAll();
	
	/**
	 * Notify all processors to stop. For any processor, once stopped it cannot be started again.
	 */
	void stopAll();
	
	/**
	 * Get the status of a processor
	 * @param processorId	id of the processor
	 * @return	status or null if the processor does not exist
	 */
	ProcessorStatus getProcessorStatus(String processorId);
	
	/**
	 * Get the status of the processors
	 * @return	status of the processors, key-ed by IDs of the processors in alphabet order
	 */
	SortedMap<String, ProcessorStatus> getProcessorStatus();
	
	/**
	 * Get processing status per stream
	 * @return	stream processing status per stream listed in the original order of those streams, key-ed by IDs of the streams
	 * @throws TransactionStorageInfrastructureException		any exception happened in transaction storage
	 * @throws DataStreamInfrastructureException				any exception happened in data stream 
	 */
	LinkedHashMap<String, StreamStatus> getStreamStatus() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException;

	/**
	 * Get the overall status of the processing
	 * @return	overall status
	 * @throws TransactionStorageInfrastructureException		any exception happened in transaction storage
	 * @throws DataStreamInfrastructureException				any exception happened in data stream 
	 */
	default Status getStatus() throws TransactionStorageInfrastructureException, DataStreamInfrastructureException{
		return new Status(getProcessorStatus(), getStreamStatus());
	}

}
