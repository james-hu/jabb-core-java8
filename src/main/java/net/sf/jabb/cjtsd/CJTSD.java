/**
 * 
 */
package net.sf.jabb.cjtsd;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is the class for handling (primarily generating) Compact JSON Time Series Data (CJTSD) data.
 * <p>To generate a CJTSD object:</p>
 * <code>
 * 	CJTSD.builder().add(...).add(...).add(...).build()
 * </code>
 * <p>To consume a CJTSD object:</p>
 * <code>
 * 	objectMapper.readValue(jsonString, CJTSD.class).toList()
 * </code>
 * @see <a href="https://github.com/james-hu/cjtsd-js/wiki/Compact-JSON-Time-Series-Data">https://github.com/james-hu/cjtsd-js/wiki/Compact-JSON-Time-Series-Data</a>
 * @author James Hu (Zhengmao Hu)
 *
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
public class CJTSD extends PlainCJTSD{

	public CJTSD(){
		super();
	}
	
	/**
	 * Shallow copy constructor
	 * @param other	another instance from which the properties will be copied
	 */
	public CJTSD(PlainCJTSD other){
		super(other);
	}
	
	/**
	 * Convert into list form
	 * @return	the list containing entries of data points
	 */
	public List<Entry> toList(){
		if (t == null || t.size() == 0){
			return Collections.emptyList();
		}
		
		List<Entry> result = new ArrayList<>(t.size());
		int lastDuration = 0;
		for (int i = 0; i < t.size(); i ++){
			long timestamp = t.get(i);
			int duration = -1;
			if (i < d.size()){
				duration = d.get(i);
			}
			if (duration == -1){
				duration = lastDuration;
			}
			lastDuration = duration;
			
			LocalDateTime timestampObj;
			Duration durationObj;
			if (u == null || u.equals("m")){
				timestampObj = LocalDateTime.ofEpochSecond(timestamp * 60, 0, ZoneOffset.UTC);
				durationObj = Duration.ofMinutes(duration);
			}else if (u.equals("s")){
				timestampObj = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);
				durationObj = Duration.ofSeconds(duration);
			}else if (u.equals("S")){
				long seconds = timestamp / 1000;
				int nano = (int)(timestamp % 1000) * 1_000_000;
				timestampObj = LocalDateTime.ofEpochSecond(seconds, nano, ZoneOffset.UTC);
				durationObj = Duration.ofMillis(duration);
			}else{
				throw new IllegalArgumentException("Unit not supported: " + u);
			}
			
			result.add(new Entry(timestampObj, durationObj,
					c == null || i >= c.size() ? null : c.get(i),
					s == null || i >= s.size() ? null : s.get(i),
					a == null || i >= a.size() ? null : a.get(i),
					m == null || i >= m.size() ? null : m.get(i),
					x == null || i >= x.size() ? null : x.get(i),
					n == null || i >= n.size() ? null : n.get(i),
					o == null || i >= o.size() ? null : o.get(i)
					));
			
		}
		return result;
	}
	
	/**
	 * Create a builder for generating CJTSD object.
	 * The expected number of data points is 50.
	 * If the actual number of data points exceeds the expected, the builder will just grow.
	 * @return	the builder
	 */
	static public Builder builder(){
		return new Builder();
	}
	
	/**
	 * Create a builder for generating CJTSD object.
	 * The expected number of data points is specified as argument to this method.
	 * If the actual number of data points exceeds the expected, the builder will just grow.
	 * @param expectedSize		the expected number of data points
	 * @return	the builder
	 */
	static public Builder builder(int expectedSize){
		return new Builder(expectedSize);
	}
	
	/**
	 * Builder that keeps intermediate data structure for creating
	 * CJTSD object.
	 * @author James Hu (Zhengmao Hu)
	 *
	 */
	static public class Builder{
		private int expectedSize;
		private ChronoUnit unit = ChronoUnit.MINUTES;
		private LongList timestamps;
		private IntList durations;
		private List<Long> counts;
		private List<Number> sums;
		private List<Number> avgs;
		private List<Number> mins;
		private List<Number> maxs;
		private List<Number> numbers;
		private List<Object> objs;

		Builder(){
			this(50);
		}
		
		Builder(int expectedSize){
			this.expectedSize = expectedSize;
			timestamps = new LongArrayList(expectedSize);
			durations = new IntArrayList(expectedSize);
		}
		
		/**
		 * Set the unit which can be one of:
		 * <ul>
		 * 	<li>MINUTES</li>
		 * 	<li>SECONDS</li>
		 * 	<li>MILLIS</li>
		 * </ul>
		 * If this method has not been called, the default unit (MINUTES) will be used.
		 * This method should be called no more than once, and only before calling other methods.
		 * @param unit the unit of the timestamps and durations of the CJTSD object to be generated
		 * @return	the builder itself
		 */
		public Builder setUnit(ChronoUnit unit){
			switch(unit){
				case MINUTES:
				case SECONDS:
				case MILLIS:
					this.unit = unit;
					return this;
				default:
					throw new IllegalArgumentException("Unit not supported: " + unit);
			}
		}
		
		/**
		 * Add a data point
		 * @param timestamp	the timestamp of the data point
		 * @param duration	the duration
		 * @return	the builder itself
		 */
		public Builder add(long timestamp, int duration){
			if (timestamps.size() == 0 && duration == -1){
				throw new IllegalArgumentException("Duration must be specified for the first data point");
			}

			timestamps.add(timestamp);
			int d = duration;
			if (durations.size() > 0){
				int n = indexOfLastSpecifiedDuration();
				if (n >= 0 && duration == durations.getInt(n)){
					d = -1;
				}
			}
			durations.add(d);
			return this;
		}
		
		/**
		 * Add a data point
		 * @param timestamp	the timestamp of the data point
		 * @param duration	the duration, can be null if the duration is the same as previous one or if this is the first data point and the duration is zero
		 * @return	the builder itself
		 */
		public Builder add(LocalDateTime timestamp, Duration duration){
			long tsLong;
			int durInt;
			switch(unit){
				case MINUTES:
					tsLong = timestamp.toEpochSecond(ZoneOffset.UTC)/60;
					durInt = duration == null ? (timestamps.size() == 0 ? 0 : -1) : (int) duration.toMinutes();
					break;
				case SECONDS:
					tsLong = timestamp.toEpochSecond(ZoneOffset.UTC);
					durInt =  duration == null ? (timestamps.size() == 0 ? 0 : -1) : (int)(duration.toMillis() / 1000);
					break;
				case MILLIS:
					tsLong = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
					durInt =  duration == null ? (timestamps.size() == 0 ? 0 : -1) : (int) duration.toMillis();
					break;
				default:
					throw new IllegalArgumentException("Unit not supported: " + unit);
			}

			return add(tsLong, durInt);
		}
		
		/**
		 * Add a data point with the same duration as its previous data point.
		 * If this is the first data point, the duration will be considered as zero.
		 * @param timestamp	the timestamp of the data point
		 * @return	the builder itself
		 */
		public Builder add(long timestamp){
			timestamps.add(timestamp);
			durations.add(timestamps.size() == 0 ? 0 : -1);
			return this;
		}
		
		/**
		 * Add a data point with the same duration as its previous data point.
		 * If this is the first data point, the duration will be considered as zero.
		 * @param timestamp	the timestamp of the data point
		 * @return	the builder itself
		 */
		public Builder add(LocalDateTime timestamp){
			return add(timestamp, null);
		}
		
		/**
		 * Add a count number ('c') to the current data point
		 * @param count	the count number
		 * @return	the builder itself
		 */
		public Builder addCount(Long count){
			if (counts == null){
				counts = new ArrayList<>(expectedSize);
			}
			counts.add(count);
			return this;
		}

		/**
		 * Add a sum number ('s') to the current data point
		 * @param sum	the sum number
		 * @return	the builder itself
		 */
		public Builder addSum(Number sum){
			if (sums == null){
				sums = new ArrayList<>(expectedSize);
			}
			sums.add(sum);
			return this;
		}

		/**
		 * Add a average number ('a') to the current data point
		 * @param avg	the average number
		 * @return	the builder itself
		 */
		public Builder addAvg(Number avg){
			if (avgs == null){
				avgs = new ArrayList<>(expectedSize);
			}
			avgs.add(avg);
			return this;
		}

		/**
		 * Add a minimal number ('m') to the current data point
		 * @param min	the minimal number
		 * @return	the builder itself
		 */
		public Builder addMin(Number min){
			if (mins == null){
				mins = new ArrayList<>(expectedSize);
			}
			mins.add(min);
			return this;
		}

		/**
		 * Add a maximal number ('x') to the current data point
		 * @param max	the maximal number
		 * @return	the builder itself
		 */
		public Builder addMax(Number max){
			if (maxs == null){
				maxs = new ArrayList<>(expectedSize);
			}
			maxs.add(max);
			return this;
		}

		/**
		 * Add a generic number ('n') to the current data point
		 * @param n	the number
		 * @return	the builder itself
		 */
		public Builder addNumber(Number n){
			if (numbers == null){
				numbers = new ArrayList<>(expectedSize);
			}
			numbers.add(n);
			return this;
		}

		/**
		 * Add an object ('o') to the current data point
		 * @param obj	the object
		 * @return	the builder itself
		 */
		public Builder addObj(Object obj){
			if (objs == null){
				objs = new ArrayList<>(expectedSize);
			}
			objs.add(obj);
			return this;
		}
		
		/**
		 * Find the index of last explicitly specified duration element
		 * @return	the index of the last explicitly specified duration element, or -1 if not found.
		 */
		private int indexOfLastSpecifiedDuration(){
			int result = -1;
			for (int i = durations.size() - 1; i >=0; i --){
				if (durations.getInt(i) != -1){
					result = i;
					break;
				}
			}
			return result;
		}
		
		/**
		 * Build the CJTSD object
		 * @return	the CJTSD object that is ready to be serialized to JSON
		 */
		public CJTSD build(){
			CJTSD result = new CJTSD();
			
			// u
			switch(unit){
				case MINUTES:
					result.u = null;  // it is the default one
					break;
				case SECONDS:
					result.u = "s";
					break;
				case MILLIS:
					result.u = "S";
					break;
				default:
					throw new IllegalArgumentException("Unit not supported: " + unit);
			}

			// t
			result.t = this.timestamps;
			
			// d
			int n = indexOfLastSpecifiedDuration();
			if (n >= 0 && n < durations.size() - 1){
				durations.size(n + 1);
			}
			for (int i = 1; i < durations.size(); i ++){
				int d = durations.getInt(i - 1);
				if (durations.getInt(i) == -1 &&  d < 100){	// no need to replace with -1 if there are only or less than two digits
					durations.set(i, d);
				}
			}
			result.d = this.durations;
			
			// c, s, a, m, x, n, o
			result.c = this.counts;
			result.s = this.sums;
			result.a = this.avgs;
			result.m = this.mins;
			result.x = this.maxs;
			result.n = this.numbers;
			result.o = this.objs;
			
			return result;
		}

	}
	
	static public class Entry{
		LocalDateTime timestamp;
		Duration duration;
		Long count;
		Number sum;
		Number avg;
		Number min;
		Number max;
		Number number;
		Object obj;

		Entry(LocalDateTime timestamp, Duration duration, Long count, Number sum, Number avg, Number min, Number max, Number number, Object obj) {
			super();
			this.timestamp = timestamp;
			this.duration = duration;
			this.count = count;
			this.sum = sum;
			this.avg = avg;
			this.min = min;
			this.max = max;
			this.number = number;
			this.obj = obj;
		}

		public LocalDateTime getTimestamp() {
			return timestamp;
		}

		public Duration getDuration() {
			return duration;
		}

		public Long getCount() {
			return count;
		}

		public Number getSum() {
			return sum;
		}

		public Number getAvg() {
			return avg;
		}

		public Number getMin() {
			return min;
		}

		public Number getMax() {
			return max;
		}

		public Number getNumber() {
			return number;
		}

		public Object getObj() {
			return obj;
		}
		
		
	}


}
