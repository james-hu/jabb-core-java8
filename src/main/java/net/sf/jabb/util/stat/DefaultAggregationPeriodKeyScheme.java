/**
 * 
 */
package net.sf.jabb.util.stat;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The scheme of year, month, day, hour, minute
 * @author James Hu
 *
 */
public class DefaultAggregationPeriodKeyScheme implements HierarchicalAggregationPeriodKeyScheme, Serializable{
	private static final long serialVersionUID = -3654502940787144075L;

	protected AggregationPeriodHierarchy<?> aph;
	protected boolean enableCompression;

	protected DefaultAggregationPeriodKeyScheme(AggregationPeriodHierarchy<?> aggregationPeriodHierarchy, boolean enableCompression){
		aggregationPeriodHierarchy.codeMapping.values().stream()
			.map(node->node.aggregationPeriodAndAttachment.aggregationPeriod).forEach(ap->{
				validateAggregationPeriod(ap);
			});
		this.aph = aggregationPeriodHierarchy;
		this.enableCompression = enableCompression;
	}
	
	/**
	 * Validate aggregation period.
	 * It ensures that the following units can only have 1 as amount: YEAR_MONTH_DAY, WEEK_BASED_YEAR_WEEK, YEAR_WEEK_ISO, YEAR_WEEK_SUNDAY_START
	 * @param ap	the aggregation period to be validated.
	 */
	static protected void validateAggregationPeriod(AggregationPeriod ap){
		switch(ap.unit){
			case YEAR_MONTH_DAY:
			case WEEK_BASED_YEAR_WEEK:
			case YEAR_WEEK_ISO:
			case YEAR_WEEK_SUNDAY_START:
				if (ap.amount != 1){
					throw new IllegalArgumentException("Aggregation periods with " + ap.unit + " as unit can only have 1 as amount: " + ap.amount);
				}
				break;
			default:
				// do nothing
		}
	}
	
	@Override
	public String generateKey(String apCode, int year, int month, int dayOfMonth, int hour, int minute) {
		return generateKey(aph.get(apCode), year, month, dayOfMonth, hour, minute);
	}
	
	@Override
	public long generateKeyNumber(String apCode, int year, int month, int dayOfMonth, int hour, int minute) {
		return generateKeyNumber(aph.get(apCode), year, month, dayOfMonth, hour, minute);
	}
	
	@Override
	public String generateKey(AggregationPeriod ap, int year, int month, int dayOfMonth, int hour, int minute) {
		return staticGenerateKey(ap, year, month, dayOfMonth, hour, minute, enableCompression);
	}
	
	@Override
	public long generateKeyNumber(AggregationPeriod ap, int year, int month, int dayOfMonth, int hour, int minute) {
		return staticGenerateKeyNumber(ap, year, month, dayOfMonth, hour, minute, enableCompression) >> 5;
	}
	
	@Override
	public String generateKey(String apCode, LocalDateTime dateTimeWithoutZone) {
		return generateKey(aph.get(apCode), dateTimeWithoutZone);
	}
		
	@Override
	public long generateKeyNumber(String apCode, LocalDateTime dateTimeWithoutZone) {
		return generateKeyNumber(aph.get(apCode), dateTimeWithoutZone);
	}
		
	@Override
	public String generateKey(AggregationPeriod ap, LocalDateTime dateTimeWithoutZone){
		return staticGenerateKey(ap, dateTimeWithoutZone, enableCompression);
	}
	
	@Override
	public long generateKeyNumber(AggregationPeriod ap, LocalDateTime dateTimeWithoutZone){
		return staticGenerateKeyNumber(ap, dateTimeWithoutZone, enableCompression) >> 5;
	}
	
	@Override
	public String generateKey(AggregationPeriod ap, long keyNumber){
		return staticGenerateKey(ap, keyNumber, enableCompression);
	}
	
	@Override
	public String generateKey(String apCode, long keyNumber){
		return staticGenerateKey(aph.get(apCode), keyNumber, enableCompression);
	}
	

	
	@Override
	public int getKeyNumberLength(AggregationPeriod ap){
		return staticGetKeyNumberLength(ap, enableCompression);
	}

	@Override
	public int getKeyNumberLength(String apCode){
		return staticGetKeyNumberLength(aph.get(apCode), enableCompression);
	}


	static protected int staticGetKeyNumberLength(AggregationPeriod ap, boolean enableCompression){
		switch(ap.unit){
			case YEAR:
			case WEEK_BASED_YEAR:
				return 4;
			case YEAR_MONTH:
				return enableCompression & ap.amount > 1 ? 5 : 6;
			case WEEK_BASED_YEAR_WEEK:
			case YEAR_WEEK_ISO:
			case YEAR_WEEK_SUNDAY_START:
				return 6;
			case YEAR_MONTH_DAY:
				return 8;
			case YEAR_MONTH_DAY_HOUR:
				return enableCompression & ap.amount > 2 ? 9 : 10;
			case YEAR_MONTH_DAY_HOUR_MINUTE:
				return enableCompression & (ap.amount == 10 || ap.amount == 20 || ap.amount >= 6) ? 11 : 12;
			default:
				throw new IllegalArgumentException("Unknown aggregation period: " + ap);
		}
	}
	
	static protected long staticGenerateKeyNumber(AggregationPeriod ap, int year, int month, int dayOfMonth, int hour, int minute, boolean enableCompression) {
		switch(ap.unit){
			case YEAR:
				return compress(enableCompression, ap, year - year % ap.amount, 4);
			case YEAR_MONTH:
				return compress(enableCompression, ap, year*100 + month - ((month - 1) % ap.amount), 6);
			case YEAR_MONTH_DAY:
				return compress(enableCompression, ap, year*10000 + month * 100 + dayOfMonth, 8);	// amount must be 1
			case YEAR_MONTH_DAY_HOUR:
				return compress(enableCompression, ap, year*1000000L + month * 10000 + dayOfMonth * 100 + hour - hour % ap.amount, 10);
			case YEAR_MONTH_DAY_HOUR_MINUTE:
				return compress(enableCompression, ap, year*100000000L + month * 1000000 + dayOfMonth * 10000 + hour * 100 + minute - minute % ap.amount, 12);
			default:
				return staticGenerateKeyNumber(ap, year, month, dayOfMonth, hour, minute, enableCompression);
		}
	}

	static protected long staticGenerateKeyNumber(AggregationPeriod ap, LocalDateTime dateTimeWithoutZone, boolean enableCompression){
		int year;
		int week;
		switch(ap.unit){
			case WEEK_BASED_YEAR:
				year = dateTimeWithoutZone.get(IsoFields.WEEK_BASED_YEAR);
				return compress(enableCompression, ap, year - year % ap.amount, 4);
			case WEEK_BASED_YEAR_WEEK:
				year = dateTimeWithoutZone.get(IsoFields.WEEK_BASED_YEAR);
				week = dateTimeWithoutZone.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
				return compress(enableCompression, ap, year*100 + week, 6);		// amount must be 1
			case YEAR_WEEK_ISO:
				year = dateTimeWithoutZone.getYear();
				week = dateTimeWithoutZone.get(WeekFields.ISO.weekOfYear());
				return compress(enableCompression, ap, year*100 + week, 6);		// amount must be 1
			case YEAR_WEEK_SUNDAY_START:
				year = dateTimeWithoutZone.getYear();
				week = dateTimeWithoutZone.get(WeekFields.SUNDAY_START.weekOfYear());
				return compress(enableCompression, ap, year*100 + week, 6);		// amount must be 1
			default:
				return staticGenerateKeyNumber(ap, dateTimeWithoutZone.getYear(), dateTimeWithoutZone.getMonthValue(), dateTimeWithoutZone.getDayOfMonth(), 
						dateTimeWithoutZone.getHour(), dateTimeWithoutZone.getMinute(), enableCompression);
		}
	}
	
	static protected String staticGenerateKey(AggregationPeriod ap, int year, int month, int dayOfMonth, int hour, int minute, boolean enableCompression) {
		return toString(ap.getCodeName(), staticGenerateKeyNumber(ap, year, month, dayOfMonth, hour, minute, enableCompression));
	}

	static protected String staticGenerateKey(AggregationPeriod ap, LocalDateTime dateTimeWithoutZone, boolean enableCompression){
		return toString(ap.getCodeName(), staticGenerateKeyNumber(ap, dateTimeWithoutZone, enableCompression));
	}
	
	static protected String staticGenerateKey(AggregationPeriod ap, long number, boolean enableCompression){
		int length = staticGetKeyNumberLength(ap, enableCompression);
		return toString(ap.getCodeName(), number, length);
	}
	
	/**
	 * Retrieve the aggregation period information from the key
	 * @param key	the key starts with aggregation period code name
	 * @return		the aggregation period, or null if not found
	 */
	@Override
	public AggregationPeriod retrieveAggregationPeriod(String key){
		int i = endOfAggregationPeriod(key);
		return i > 0 ? AggregationPeriod.parse(key.substring(0, i)) : null;
	}
	
	/**
	 * Separate the part representing AggregationPeriod from the key
	 * @param key	the key starts with aggregation period code name
	 * @return	An array that the first element is the code name of the AggregationPeriod or null if something went wrong, 
	 * 			and the second element is the remaining part of the key
	 */
	@Override
	public String[] separateAggregationPeriod(String key){
		return staticSeparateAggregationPeriod(key);
	}
	
	/**
	 * Separate the part representing AggregationPeriod from the key
	 * @param key	the key starts with aggregation period code name
	 * @return	An array that the first element is the code name of the AggregationPeriod or null if something went wrong, 
	 * 			and the second element is the remaining part of the key
	 */
	static public String[] staticSeparateAggregationPeriod(String key){
		int i = endOfAggregationPeriod(key);
		if ( i > 0){
			return new String[] {key.substring(0, i), key.substring(i)};
		}else{
			return new String[] {null, key};
		}
	}
	
	/**
	 * Find the end position of the aggregation period code
	 * @param key	the key starts with aggregation period code name
	 * @return the next position after the last character of aggregation period code name, or -1 if not found
	 */
	static protected int endOfAggregationPeriod(String key){
		for (int i = key.length() - 1; i >= 0; i --){
			if (!Character.isDigit(key.charAt(i))){
				return i + 1;
			}
		}
		return -1;
	}
	

	/**
	 * Get the start time (inclusive) of the time period represented by the key.
	 * It accepts keys generated with any aggregation period.
	 * The key always marks the start time so there is no time zone information needed as argument.
	 * @param key	the time period key
	 * @return	the start time (inclusive) of the time period. It should be interpreted as in the same time zone in which the key is generated.
	 */
	@Override
	public LocalDateTime getStartTime(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return getStartTime(ap, key, enableCompression);
	}		
		
	static protected LocalDateTime getStartTime(AggregationPeriod ap, String key, boolean uncompress){
		long k = Long.parseLong(key.substring(ap.getCodeName().length()));
		if (uncompress){
			k = uncompress(ap, k);
		}
		int i = (int)k;
		StringBuilder sb;
		switch(ap.unit){
			case YEAR:
				return LocalDateTime.of(i, 1, 1, 0, 0);
			case YEAR_MONTH:
				return LocalDateTime.of(i / 100, i % 100, 1, 0, 0);
			case YEAR_MONTH_DAY:
				return LocalDateTime.of(i / 10000, (i % 10000)/100, i % 100, 0, 0);
			case YEAR_MONTH_DAY_HOUR:
				return LocalDateTime.of((int)(k/1000000), (int)(k % 1000000)/10000, (int)(k % 10000) / 100, (int)(k % 100), 0);
			case YEAR_MONTH_DAY_HOUR_MINUTE:
				return LocalDateTime.of((int)(k/100000000L), (int)((k % 100000000L)/1000000), (int)(k % 1000000)/10000, (int)(k % 10000) / 100, (int)(k % 100));
			case WEEK_BASED_YEAR:
				sb = new StringBuilder();
				sb.append(i); // year
				sb.append("-W01-1");
				return LocalDateTime.parse(sb.toString(), DateTimeFormatter.ISO_WEEK_DATE);
			case WEEK_BASED_YEAR_WEEK:
				sb = new StringBuilder();
				sb.append(i/100); // year
				sb.append(toString("-W", i % 100, 2)); // week;
				sb.append("-1");
				return LocalDate.parse(sb.toString(), DateTimeFormatter.ISO_WEEK_DATE).atTime(0, 0);
			case YEAR_WEEK_ISO:
				return LocalDateTime.of(i/100, 1, 1, 0, 0).with(WeekFields.ISO.weekOfYear(), i % 100);
			case YEAR_WEEK_SUNDAY_START:
				return LocalDateTime.of(i/100, 1, 1, 0, 0).with(WeekFields.SUNDAY_START.weekOfYear(), i % 100);
			default:
				throw new IllegalArgumentException("Unknown aggregation period unit: " + ap.unit);
		}
	}

	@Override
	public ZonedDateTime getEndTime(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return getEndTime(ap, key, enableCompression);
	}
	
	static protected ZonedDateTime getEndTime(AggregationPeriod ap, String key, boolean enableCompression) {
		ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(ap, key, enableCompression), ap.zone);
		ZonedDateTime nextStart = thisStart.plus(ap.amount, ap.unit.getTemporalUnit());
		return nextStart;
	}

	/**
	 * Iterate along time to find next key
	 * @param ap	the aggregation period
	 * @param key	the key at the start point
	 * @param step	step to move forward (if positive) or backward (if negative)
	 * @param unit	unit of the step
	 * @param zone	the time zone
	 * @param enableCompression	whether to apply compression or not
	 * @return	the first key found that is different form the key at the start point
	 */
	protected static String findNextKey(AggregationPeriod ap, String key, int step, TemporalUnit unit, ZoneId zone, boolean enableCompression){
		for (ZonedDateTime time = ZonedDateTime.of(getStartTime(ap, key, enableCompression), zone).plus(step, unit);; time = time.plus(step, unit)){
			String nextKey = staticGenerateKey(ap, time.toLocalDateTime(), enableCompression);
			if (!nextKey.equals(key)){
				return nextKey;
			}
		}
	}

	@Override
	public String previousKey(String key){
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return previousKey(ap, key, enableCompression);
	}
	
	static protected String previousKey(AggregationPeriod ap, String key, boolean enableCompression){
		String apCode;
		int year;
		int week;
		switch(ap.unit){
			case WEEK_BASED_YEAR:
			case YEAR:
				// this is a performance optimization
				apCode = ap.getCodeName();
				year = Integer.parseInt(key.substring(apCode.length()));
				if (enableCompression){
					year = (int)uncompress(ap, year);
				}
				year -= ap.amount;
				return toString(enableCompression, ap, year, 4);
			case YEAR_WEEK_ISO:
			case YEAR_WEEK_SUNDAY_START:
				// if the week is the first of the year, we have to iterate through days
				apCode = ap.getCodeName();
				week = Integer.parseInt(key.substring(apCode.length() + 4, key.length()));
				if (enableCompression){
					week = (int)uncompress(ap, week);
				}
				if (week <= 1){
					return findNextKey(ap, key, -1, ChronoUnit.DAYS, ap.zone, enableCompression);
				}
				// else fall down
			case WEEK_BASED_YEAR_WEEK:
				// fall down
			default:
				// it is safe to simply jump to the start of previous period
				ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(ap, key, enableCompression), ap.zone);
				ZonedDateTime previousStart = thisStart.plus(-ap.amount, ap.unit.getTemporalUnit());
				return staticGenerateKey(ap, previousStart.toLocalDateTime(), enableCompression);
		}
	}
	
	@Override
	public String nextKey(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		return nextKey(ap, key, enableCompression);
	}
	
	static protected String nextKey(AggregationPeriod ap, String key, boolean enableCompression) {
		String apCode;
		int year;
		int week;
		switch(ap.unit){
			case WEEK_BASED_YEAR:
			case YEAR:
				// this is a performance optimization
				apCode = ap.getCodeName();
				year = Integer.parseInt(key.substring(apCode.length()));
				if (enableCompression){
					year = (int)uncompress(ap, year);
				}
				year += ap.amount;
				return toString(enableCompression, ap, year, 4);
			case YEAR_WEEK_ISO:
			case YEAR_WEEK_SUNDAY_START:
				// if the week is the first of the year, we have to iterate through days
				apCode = ap.getCodeName();
				week = Integer.parseInt(key.substring(apCode.length() + 4, key.length()));
				if (enableCompression){
					week = (int)uncompress(ap, week);
				}
				if (week >= 51){
					return findNextKey(ap, key, 1, ChronoUnit.DAYS, ap.zone, enableCompression);
				}
				// else fall down
			case WEEK_BASED_YEAR_WEEK:
				// fall down
			default:
				// it is safe to simply jump to the start of previous period
				ZonedDateTime thisStart = ZonedDateTime.of(getStartTime(ap, key, enableCompression), ap.zone);
				ZonedDateTime nextStart = thisStart.plus(ap.amount, ap.unit.getTemporalUnit());
				return staticGenerateKey(ap, nextStart.toLocalDateTime(), enableCompression);
		}
	}


	@Override
	public String upperLevelKey(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		Set<AggregationPeriod> uaps = aph.getUpperLevelAggregationPeriods(ap);
		if (uaps.size() > 0){
			AggregationPeriod uap = uaps.iterator().next();
			return generateKey(uap, getStartTime(ap, key, enableCompression));
		}else{
			return null;
		}
	}

	@Override
	public List<String> upperLevelKeys(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		LocalDateTime startTime = getStartTime(ap, key, enableCompression);
		Set<AggregationPeriod> uaps = aph.getUpperLevelAggregationPeriods(ap);
		return uaps.stream().map(p->generateKey(p, startTime)).collect(Collectors.toList());
	}

	@Override
	public String firstLowerLevelKey(String key) {
		AggregationPeriod ap = retrieveAggregationPeriod(key);
		AggregationPeriod lap = aph.getLowerLevelAggregationPeriod(ap);
		return generateKey(lap, getStartTime(ap, key, enableCompression));
	}


	@Override
	public String toString(){
		return super.toString();
	}

	/**
	 * Take a non-negative number, possibly transform it to a smaller number according to the aggregation period,
	 * then convert the result to a fixed-length string format. For internal usage.
	 * @param useCompression		use compression or not
	 * @param period				the aggregation period
	 * @param originalNumber		the number to be converted, must not be negative
	 * @param numberLength			required length of the number in the returned string, 
	 * 								if the number is shorter than this length, it will be left padded with '0's
	 * @return		the string representing the number with possible leading zeros. 
	 * 				Length of the string may be greater than numberLength+prefix.length() if the number is too large to be fitted within numberLength.
	 * 				Length of the string may be greater than numberLength+prefix.length() if the transformation of the number makes its length shorter.
	 */
	protected static String toString(boolean useCompression, AggregationPeriod period, long originalNumber, int numberLength){
		long number;
		if (!useCompression || period.amount == 1){
			number = originalNumber;
		}else{
			number = compress(period, originalNumber);
			numberLength += (int)number & 0x1F;
			number >>>= 5;
		}
		
		String str = Long.toString(number);
	    int len = str.length();
	    StringBuilder sb = new StringBuilder();
	    sb.append(period.getCodeName());
	    for(int i = numberLength; i > len; i--){
	        sb.append('0');
	    }
	    sb.append(str);
	    return sb.toString();  

	}
	
	/**
	 * Convert a non-negative number to a fixed-length string format. For internal usage.
	 * @param prefix				the prefix to be appended
	 * @param number				the potentially compressed form of the key shifted 5 bits towards higher end, and the delta of key length in the lower 5 bits
	 * @return		the string representing the number with possible leading zeros. 
	 * 				Length of the string may be greater than numberLength+prefix.length() if the number is too large to be fitted within numberLength.
	 */
	protected static String toString(String prefix, long number){
		int numberLength = (int)number & 0x1F;
		number >>>= 5;
		
		String str = Long.toString(number);
	    int len = str.length();
	    StringBuilder sb = new StringBuilder();
	    sb.append(prefix);
	    for(int i = numberLength; i > len; i--){
	        sb.append('0');
	    }
	    sb.append(str);
	    return sb.toString();  
	}
	

	 /** Convert a non-negative number to a fixed-length string format. For internal usage.
	 * @param prefix				the prefix to be appended
	 * @param nonNegativeNumber		the number to be converted, must not be negative
	 * @param numberLength			required length of the number in the returned string, 
	 * 								if the number is shorter than this length, it will be left padded with '0's
	 * @return		the string representing the number with possible leading zeros. 
	 * 				Length of the string may be greater than numberLength+prefix.length() if the number is too large to be fitted within numberLength.
	 */
	protected static String toString(String prefix, long nonNegativeNumber, int numberLength){
		String str = Long.toString(nonNegativeNumber);
	    int len = str.length();

	    StringBuilder sb = new StringBuilder();
	    sb.append(prefix);
	    for(int i = numberLength; i > len; i--){
	        sb.append('0');
	    }
	    sb.append(str);
	    return sb.toString();       
	}

	protected static long compress(boolean enableCompression, AggregationPeriod ap, long x, int length){
		if (enableCompression){
			long compressed = compress(ap, x);
			return (compressed & 0xFFFFFFFFFFFFFFE0L) | ((((int)compressed & 0x1F) + length) & 0x1F);
		}else{
			return (x << 5) | (length & 0x1F);
		}
	}
	
	/**
	 * Compress the number part of the key
	 * @param ap	the aggregation period
	 * @param x		the original form of the key
	 * @return		the potentially compressed form of the key shifted 5 bits towards higher end, and the delta of key length in the lower 5 bits
	 */
	protected static long compress(AggregationPeriod ap, long x){
		int lengthDelta = 0;
		int amount = ap.amount;
		long compressed = x;
		if (amount != 1){
			switch(ap.unit){
				case YEAR:
				case WEEK_BASED_YEAR:
					compressed = x / amount;
					break;
				case YEAR_MONTH:
					compressed = (x / 100) * 10 + ((x % 100) - 1) / amount; // converted month to 0 based and reduced 1 digit
					lengthDelta --;
					break;
				case YEAR_MONTH_DAY_HOUR:
					if (amount == 2){
						compressed = x / amount;
					}else{
						compressed = (x / 100) * 10 + (x % 100) / amount; // reduced 1 digit (24/3=8)
						lengthDelta --;
					}
					break;
				case YEAR_MONTH_DAY_HOUR_MINUTE:
					if (amount == 2 || amount == 4 || amount ==5){
						compressed = x / amount;
					}else if (amount == 10 || amount == 20){
						compressed = x / amount;
						lengthDelta --;
					}else if (amount >= 6){
						compressed = (x / 100) * 10 + (x % 100) / amount; // reduced 1 digit (60/6=10)
						lengthDelta --;
					}else{	// < 6
						compressed = (x / 100) * 100 + (x % 100) / amount; 
					}
					break;
				case YEAR_MONTH_DAY:
				case WEEK_BASED_YEAR_WEEK:
				case YEAR_WEEK_ISO:
				case YEAR_WEEK_SUNDAY_START:
				default:
					// do nothing
			}
		}
		return (compressed << 5) | (lengthDelta & 0x1F);
	}
	
	/**
	 * Uncompress the number part of the key
	 * @param ap	the aggregation period
	 * @param x		potentially compressed form of the number part
	 * @return		the original form of the number part
	 */
	protected static long uncompress(AggregationPeriod ap, long x){
		if (ap.amount == 1){
			return x;
		}
		int amount = ap.amount;
		switch(ap.unit){
			case YEAR:
			case WEEK_BASED_YEAR:
				return x * amount;
			case YEAR_MONTH:
				return (x / 10) * 100 + 1 + (x % 10) * amount;
			case YEAR_MONTH_DAY_HOUR:
				if (amount == 2){
					return x * amount;
				}else{
					return (x / 10) * 100 + (x % 10) * amount;
				}
			case YEAR_MONTH_DAY_HOUR_MINUTE:
				if (amount == 2 || amount == 4 || amount ==5 || amount == 10 || amount == 20){
					return x * amount;
				}else if (amount >= 6){
					return (x / 10) * 100 + (x % 10) * amount;
				}else{	// < 6
					return (x / 100) * 100 + (x % 100) * amount; 
				}
			case YEAR_MONTH_DAY:
			case WEEK_BASED_YEAR_WEEK:
			case YEAR_WEEK_ISO:
			case YEAR_WEEK_SUNDAY_START:
			default:
				return x;
		}
	}
	
	/**
	 * Create a hierarchical instance
	 * Compression in the created instance will be disabled.
	 * @param aph	the hierarchy of aggregation periods
	 * @return	the HierarchicalAggregationPeriodKeyScheme
	 */
	static public HierarchicalAggregationPeriodKeyScheme newInstance(AggregationPeriodHierarchy<?> aph){
		return new DefaultAggregationPeriodKeyScheme(aph, false);
	}

	/**
	 * Create a hierarchical instance
	 * @param aph	the hierarchy of aggregation periods
	 * @param enableCompression	Whether the keys should be compressed to make them more compact
	 * @return	the HierarchicalAggregationPeriodKeyScheme
	 */
	static public HierarchicalAggregationPeriodKeyScheme newInstance(AggregationPeriodHierarchy<?> aph, boolean enableCompression){
		return new DefaultAggregationPeriodKeyScheme(aph, enableCompression);
	}

	/**
	 * Create an non-hierarchical instance specific to an aggregation period.
	 * Compression in the created instance will be disabled.
	 * @param ap	the aggregation period
	 * @return		a AggregationPeriodKeyScheme specific to the aggregation period 
	 */
	static public AggregationPeriodKeyScheme newInstance(AggregationPeriod ap){
		return newInstance(ap, false);
	}
	
	/**
	 * Create an non-hierarchical instance specific to an aggregation period.
	 * @param ap	the aggregation period
	 * @param enableCompression	Whether the keys should be compressed to make them more compact
	 * @return		a AggregationPeriodKeyScheme specific to the aggregation period 
	 */
	static public AggregationPeriodKeyScheme newInstance(AggregationPeriod ap, boolean enableCompression){
		validateAggregationPeriod(ap);

		return new AggregationPeriodKeyScheme(){

			@Override
			public LocalDateTime getStartTime(String key) {
				return DefaultAggregationPeriodKeyScheme.getStartTime(ap, key, enableCompression);
			}

			@Override
			public ZonedDateTime getEndTime(String key) {
				return DefaultAggregationPeriodKeyScheme.getEndTime(ap, key, enableCompression);
			}

			@Override
			public String previousKey(String key) {
				return DefaultAggregationPeriodKeyScheme.previousKey(ap, key, enableCompression);
			}

			@Override
			public String generateKey(int year, int month, int dayOfMonth, int hour, int minute) {
				return DefaultAggregationPeriodKeyScheme.staticGenerateKey(ap, year, month, dayOfMonth, hour, minute, enableCompression);
			}

			@Override
			public long generateKeyNumber(int year, int month, int dayOfMonth, int hour, int minute) {
				return DefaultAggregationPeriodKeyScheme.staticGenerateKeyNumber(ap, year, month, dayOfMonth, hour, minute, enableCompression) >>> 5;
			}

			@Override
			public String generateKey(LocalDateTime dateTimeWithoutZone) {
				return DefaultAggregationPeriodKeyScheme.staticGenerateKey(ap, dateTimeWithoutZone, enableCompression);
			}

			@Override
			public long generateKeyNumber(LocalDateTime dateTimeWithoutZone) {
				return DefaultAggregationPeriodKeyScheme.staticGenerateKeyNumber(ap, dateTimeWithoutZone, enableCompression) >>> 5;
			}

			@Override
			public String[] separateAggregationPeriod(String key) {
				return DefaultAggregationPeriodKeyScheme.staticSeparateAggregationPeriod(key);
			}
			
			@Override
			public int getKeyNumberLength(){
				return DefaultAggregationPeriodKeyScheme.staticGetKeyNumberLength(ap, enableCompression);
			}
			
			@Override
			public String generateKey(long keyNumber) {
				return DefaultAggregationPeriodKeyScheme.staticGenerateKey(ap, keyNumber, enableCompression);
			}


		};
	}

}
