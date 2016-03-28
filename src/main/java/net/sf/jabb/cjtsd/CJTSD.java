/**
 * 
 */
package net.sf.jabb.cjtsd;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * @author James Hu (Zhengmao Hu)
 *
 */
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
public class CJTSD {
	String u;
	LongList t;
	IntList d;
	
	List<Long> c;
	List<Number> s;
	List<Number> a;
	List<Number> m;
	List<Number> x;
	List<Number> n;
	List<Object> o;

	CJTSD(){
		
	}
	
	static public Builder builder(){
		return new Builder();
	}
	
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
		private List<Number> ns;
		private List<Object> objs;

		Builder(){
			this(50);
		}
		
		Builder(int expectedSize){
			this.expectedSize = expectedSize;
			timestamps = new LongArrayList(expectedSize);
			durations = new IntArrayList(expectedSize);
		}
		
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
		
		public Builder add(long timestamp, int duration){
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
		
		public Builder add(LocalDateTime timestamp, Duration duration){
			long tsLong;
			int durInt;
			switch(unit){
				case MINUTES:
					tsLong = timestamp.toEpochSecond(ZoneOffset.UTC)/60;
					durInt = (int) duration.toMinutes();
					break;
				case SECONDS:
					tsLong = timestamp.toEpochSecond(ZoneOffset.UTC);
					durInt = (int)(duration.toMillis() / 1000);
					break;
				case MILLIS:
					tsLong = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
					durInt = (int) duration.toMillis();
					break;
				default:
					throw new IllegalArgumentException("Unit not supported: " + unit);
			}

			return add(tsLong, durInt);
		}
		
		public Builder add(long timestamp){
			timestamps.add(timestamp);
			durations.add(-1);
			return this;
		}
		
		public Builder add(LocalDateTime timestamp){
			long tsLong;
			switch(unit){
				case MINUTES:
					tsLong = timestamp.toEpochSecond(ZoneOffset.UTC)/60;
					break;
				case SECONDS:
					tsLong = timestamp.toEpochSecond(ZoneOffset.UTC);
					break;
				case MILLIS:
					tsLong = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
					break;
				default:
					throw new IllegalArgumentException("Unit not supported: " + unit);
			}

			return add(tsLong);
		}
		
		public Builder addCount(Long count){
			if (counts == null){
				counts = new ArrayList<>(expectedSize);
			}
			counts.add(count);
			return this;
		}

		public Builder addSum(Number sum){
			if (sums == null){
				sums = new ArrayList<>(expectedSize);
			}
			sums.add(sum);
			return this;
		}

		public Builder addAvg(Number avg){
			if (avgs == null){
				avgs = new ArrayList<>(expectedSize);
			}
			avgs.add(avg);
			return this;
		}

		public Builder addMin(Number min){
			if (mins == null){
				mins = new ArrayList<>(expectedSize);
			}
			mins.add(min);
			return this;
		}

		public Builder addMax(Number max){
			if (maxs == null){
				maxs = new ArrayList<>(expectedSize);
			}
			maxs.add(max);
			return this;
		}

		public Builder addNumber(Number n){
			if (ns == null){
				ns = new ArrayList<>(expectedSize);
			}
			ns.add(n);
			return this;
		}

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
		
		public CJTSD build(){
			CJTSD result = new CJTSD();
			
			// u
			switch(unit){
				case MINUTES:
					result.u = "m";
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
			result.n = this.ns;
			result.o = this.objs;
			
			return result;
		}

	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public String getU() {
		return u;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public LongList getT() {
		return t;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public IntList getD() {
		return d;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public List<Long> getC() {
		return c;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public List<Number> getS() {
		return s;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public List<Number> getA() {
		return a;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public List<Number> getM() {
		return m;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public List<Number> getX() {
		return x;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public List<Number> getN() {
		return n;
	}

	@org.boon.json.annotations.JsonInclude(org.boon.json.annotations.JsonInclude.Include.NON_NULL)
	public List<Object> getO() {
		return o;
	}
}
