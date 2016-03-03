/**
 * 
 */
package net.sf.jabb.util.stat;

import static org.junit.Assert.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.SortedSet;

import org.junit.Before;
import org.junit.Test;

/**
 * @author James Hu
 *
 */
public class DefaultAggregationPeriodKeySchemeEnableCompressionTest {
	static ZoneId UTC = ZoneId.of("UTC");
	static ZoneId GMT6 = ZoneId.of("GMT-6");
	static ZoneId GMT3 = ZoneId.of("GMT-3");
	
	static String APC_1MIN = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE);
	static String APC_1MONTH = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH);
	static String APC_2MONTH = AggregationPeriod.getCodeName(2, AggregationPeriodUnit.YEAR_MONTH);
	static String APC_3MONTH = AggregationPeriod.getCodeName(3, AggregationPeriodUnit.YEAR_MONTH);
	static String APC_1YEAR = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR);

	static String APC_1MIN_MEL = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, ZoneId.of("Australia/Melbourne"));
	static String APC_5MIN_MEL = AggregationPeriod.getCodeName(5, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, ZoneId.of("Australia/Melbourne"));
	static String APC_15MIN_MEL = AggregationPeriod.getCodeName(15, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, ZoneId.of("Australia/Melbourne"));
	static String APC_20MIN_MEL = AggregationPeriod.getCodeName(20, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, ZoneId.of("Australia/Melbourne"));
	static String APC_30MIN_MEL = AggregationPeriod.getCodeName(30, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, ZoneId.of("Australia/Melbourne"));
	static String APC_1MONTH_MEL = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH, ZoneId.of("Australia/Melbourne"));
	static String APC_6MONTH_MEL = AggregationPeriod.getCodeName(6, AggregationPeriodUnit.YEAR_MONTH, ZoneId.of("Australia/Melbourne"));
	static String APC_1YEAR_MEL = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR, ZoneId.of("Australia/Melbourne"));
	static String APC_5YEAR_MEL = AggregationPeriod.getCodeName(5, AggregationPeriodUnit.YEAR, ZoneId.of("Australia/Melbourne"));

	static String APC_5MIN = AggregationPeriod.getCodeName(5, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE);
	static String APC_1HOUR = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR);
	static String APC_6HOUR = AggregationPeriod.getCodeName(6, AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR);
	static String APC_1DAY = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_MONTH_DAY);
	static String APC_1WEEK_ISO = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_WEEK_ISO);
	static String APC_1WEEK_SUNDAY_START = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.YEAR_WEEK_SUNDAY_START);

	static String APC_1WEEK_BASED_YEAR_WEEK = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.WEEK_BASED_YEAR_WEEK);
	static String APC_1WEEK_BASED_YEAR = AggregationPeriod.getCodeName(1, AggregationPeriodUnit.WEEK_BASED_YEAR);

	AggregationPeriodHierarchy aph;
	HierarchicalAggregationPeriodKeyScheme hapks;
	
	@Before
	public void setup(){
		aph = new AggregationPeriodHierarchy();
		aph.add(APC_5MIN);
			aph.add(APC_5MIN, APC_1HOUR);
				aph.add(APC_1HOUR, APC_6HOUR);
				aph.add(APC_1HOUR, APC_1DAY);
					aph.add(APC_1DAY, APC_1WEEK_ISO);
					aph.add(APC_1DAY, APC_1WEEK_SUNDAY_START);
		
		aph.add(APC_1MIN);
			aph.add(APC_1MIN, APC_1MONTH);
				aph.add(APC_1MONTH, APC_1YEAR);

		aph.add(APC_1WEEK_BASED_YEAR_WEEK);
			aph.add(APC_1WEEK_BASED_YEAR_WEEK, APC_1WEEK_BASED_YEAR);
		
		hapks = DefaultAggregationPeriodKeyScheme.newInstance(aph, true);
	}
	
	@Test
	public void testMonth(){
		AggregationPeriodHierarchy<?> aph;
		aph = new AggregationPeriodHierarchy<>();
		aph.add(APC_1MIN);
			aph.add(APC_1MIN, APC_1MONTH);
				aph.add(APC_1MONTH, APC_2MONTH);
				aph.add(APC_1MONTH, APC_3MONTH);

		HierarchicalAggregationPeriodKeyScheme hapks;
		hapks = DefaultAggregationPeriodKeyScheme.newInstance(aph, true);

		LocalDateTime ldt = LocalDateTime.parse("201503121703", DateTimeFormatter.ofPattern("uuuuMMddHHmm"));
		
		assertEquals(APC_1MONTH + "201503", hapks.generateKey(APC_1MONTH, ldt));
		assertEquals(APC_2MONTH + "20151", hapks.generateKey(APC_2MONTH, ldt));
		assertEquals(APC_3MONTH + "20150", hapks.generateKey(APC_3MONTH, ldt));
		
		assertEquals(APC_2MONTH + "20152", hapks.nextKey(APC_2MONTH + "20151"));
		assertEquals(APC_2MONTH + "20150", hapks.previousKey(APC_2MONTH + "20151"));

		assertEquals(APC_3MONTH + "20151", hapks.nextKey(APC_3MONTH + "20150"));
		assertEquals(APC_3MONTH + "20143", hapks.previousKey(APC_3MONTH + "20150"));
	}
	
	@Test
	public void testWithTimeZones() {
		LocalDateTime ldt = LocalDateTime.parse("201503121703", DateTimeFormatter.ofPattern("uuuuMMddHHmm"));
		
		assertEquals(APC_1HOUR + "2015031217", hapks.generateKey(APC_1HOUR, ldt));
		assertEquals(APC_1HOUR + "2015031221", hapks.nextKey(APC_1HOUR + "2015031220"));
		assertEquals(APC_1HOUR + "2015031219", hapks.previousKey(APC_1HOUR + "2015031220"));
		assertEquals(APC_1HOUR + "2015030923", hapks.previousKey(APC_1HOUR + "2015031000"));

	}
	
	@Test
	public void testRetrievingAggregationPeriods() {
		LocalDateTime ldt = LocalDateTime.parse("201503121703", DateTimeFormatter.ofPattern("uuuuMMddHHmm"));
		
		assertArrayEquals(new String[]{APC_1HOUR, "2015031217"}, hapks.separateAggregationPeriod(hapks.generateKey(APC_1HOUR, ldt)));
		assertArrayEquals(new String[]{APC_1HOUR, "2015031221"}, hapks.separateAggregationPeriod(hapks.nextKey(APC_1HOUR + "2015031220")));
		assertArrayEquals(new String[]{APC_1HOUR, "2015031219"}, hapks.separateAggregationPeriod(hapks.previousKey(APC_1HOUR + "2015031220")));
		assertArrayEquals(new String[]{APC_1HOUR, "2015030923"}, hapks.separateAggregationPeriod(hapks.previousKey(APC_1HOUR + "2015031000")));
		
		assertEquals(AggregationPeriod.parse(APC_1HOUR), hapks.retrieveAggregationPeriod(hapks.previousKey(APC_1HOUR + "2015031000")));
	}
	

	protected void print(AggregationPeriodKeyScheme scheme, String key){
		System.out.println(key + " -> " + scheme.getStartTime(key) + ", " + scheme.getStartTime(key) + " => " + scheme.nextKey(key));
	}
	
	@Test
	public void testInHierarchy(){
		LocalDateTime ldt = LocalDateTime.parse("201503121703", DateTimeFormatter.ofPattern("uuuuMMddHHmm"));
		assertEquals(APC_1MIN + "201503121703", hapks.generateKey(APC_1MIN, ldt));

		assertEquals(APC_1MONTH + "201503", hapks.upperLevelKey(APC_1MIN + "201503121703"));
		assertEquals(APC_1YEAR + "2015", hapks.upperLevelKey(APC_1MONTH + "201503"));
		
		assertEquals(APC_1MONTH + "201501", hapks.firstLowerLevelKey(APC_1YEAR + "2015"));
		assertEquals(APC_1MIN + "201503010000", hapks.firstLowerLevelKey(APC_1MONTH + "201503"));
		
		List<String> keys = hapks.upperLevelKeys(APC_1HOUR + "2015031217");
		assertNotNull(keys);
		assertEquals(2, keys.size());
		assertEquals(APC_6HOUR + "201503122", keys.get(0));
		assertEquals(APC_1DAY + "20150312", keys.get(1));
		
		testRoundTrip(APC_1MIN + "201503121703");
		testRoundTrip(APC_1MONTH + "201503");
		testRoundTrip(APC_1YEAR + "2015");
		testRoundTrip(APC_1MONTH + "201501");
		testRoundTrip(APC_1MIN + "201503010000");
		testRoundTrip(APC_6HOUR + "201503122");
		testRoundTrip(APC_1DAY + "20150312");
	}
	
	@Test
	public void testWeeks(){
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200852", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2008-12-28T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2008-12-29T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2008-12-31T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2009-01-04T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200902", hapks.generateKey(APC_1WEEK_BASED_YEAR_WEEK, LocalDateTime.parse("2009-01-05T10:00")));
		
		assertEquals(APC_1WEEK_BASED_YEAR + "2008", hapks.generateKey(APC_1WEEK_BASED_YEAR, LocalDateTime.parse("2008-12-28T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR + "2009", hapks.generateKey(APC_1WEEK_BASED_YEAR, LocalDateTime.parse("2008-12-29T10:00")));
		assertEquals(APC_1WEEK_BASED_YEAR + "2009", hapks.generateKey(APC_1WEEK_BASED_YEAR, LocalDateTime.parse("2009-01-04T10:00")));
		
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200901", hapks.nextKey(APC_1WEEK_BASED_YEAR_WEEK + "200852"));
		assertEquals(APC_1WEEK_BASED_YEAR_WEEK + "200852", hapks.previousKey(APC_1WEEK_BASED_YEAR_WEEK + "200901"));
		
		
		assertEquals(APC_1WEEK_ISO + "200852", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2008-12-28T10:00")));
		assertEquals(APC_1WEEK_ISO + "200853", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2008-12-29T10:00")));
		assertEquals(APC_1WEEK_ISO + "200853", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2008-12-31T10:00")));
		
		assertEquals(APC_1WEEK_ISO + "200901", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2009-01-01T10:00")));
		assertEquals(APC_1WEEK_SUNDAY_START + "200901", hapks.generateKey(APC_1WEEK_SUNDAY_START, LocalDateTime.parse("2009-01-01T10:00")));

		assertEquals(APC_1WEEK_ISO + "201000", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2010-01-01T10:00")));
		assertEquals(APC_1WEEK_SUNDAY_START + "201001", hapks.generateKey(APC_1WEEK_SUNDAY_START, LocalDateTime.parse("2010-01-01T10:00")));

		assertEquals(APC_1WEEK_ISO + "201000", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2010-01-03T10:00")));
		assertEquals(APC_1WEEK_ISO + "201001", hapks.generateKey(APC_1WEEK_ISO, LocalDateTime.parse("2010-01-04T10:00")));

		assertEquals(APC_1DAY + "20100101", hapks.generateKey(APC_1DAY, hapks.getStartTime(APC_1WEEK_ISO + "201000")));
		assertEquals(APC_1DAY + "20100101", hapks.generateKey(APC_1DAY, hapks.getStartTime(APC_1WEEK_SUNDAY_START + "201001")));
		
		testRoundTrip(APC_1WEEK_ISO + "200852");
		testRoundTrip(APC_1WEEK_ISO + "200853");
		testRoundTrip(APC_1WEEK_ISO + "201000");
		testRoundTrip(APC_1WEEK_ISO + "201001");

		testRoundTrip(APC_1WEEK_SUNDAY_START + "200852");
		testRoundTrip(APC_1WEEK_SUNDAY_START + "200853");
		testRoundTrip(APC_1WEEK_SUNDAY_START + "201001");
		
		testRoundTrip(APC_1WEEK_BASED_YEAR_WEEK + "200852");
		testRoundTrip(APC_1WEEK_BASED_YEAR_WEEK + "201001");
	}
	
	protected void testRoundTrip(String key){
		assertEquals(key, hapks.nextKey(hapks.previousKey(key)));
		assertEquals(key, hapks.previousKey(hapks.nextKey(key)));
	}
	
	@Test
	public void testRetrieveAggregationPeriodsWithTimeZone(){
		AggregationPeriodHierarchy<?> aph;
		aph = new AggregationPeriodHierarchy<>();
		aph.add(APC_1MIN_MEL);
			aph.add(APC_1MIN_MEL, APC_1MONTH_MEL);
			aph.add(APC_1MIN_MEL, APC_5MIN_MEL);
				aph.add(APC_5MIN_MEL, APC_15MIN_MEL);
				aph.add(APC_5MIN_MEL, APC_20MIN_MEL);
				aph.add(APC_15MIN_MEL, APC_30MIN_MEL);
				aph.add(APC_1MONTH_MEL, APC_1YEAR_MEL);
				aph.add(APC_1MONTH_MEL, APC_6MONTH_MEL);
				aph.add(APC_1MONTH_MEL, APC_5YEAR_MEL);

		HierarchicalAggregationPeriodKeyScheme hapks;
		hapks = DefaultAggregationPeriodKeyScheme.newInstance(aph, true);

		AggregationPeriod ap = hapks.retrieveAggregationPeriod("ok1N201603031249");
		assertNotNull(ap);
		assertEquals(ap.getZone(), ZoneId.of("Australia/Melbourne"));
		assertEquals(1, ap.getAmount());
		assertEquals(AggregationPeriodUnit.YEAR_MONTH_DAY_HOUR_MINUTE, ap.getUnit());
		
		ap = hapks.retrieveAggregationPeriod("ok1M201603");
		assertNotNull(ap);
		assertEquals(ap.getZone(), ZoneId.of("Australia/Melbourne"));
		assertEquals(1, ap.getAmount());
		assertEquals(AggregationPeriodUnit.YEAR_MONTH, ap.getUnit());

		ap = hapks.retrieveAggregationPeriod("ok1Y2016");
		assertNotNull(ap);
		assertEquals(ap.getZone(), ZoneId.of("Australia/Melbourne"));
		assertEquals(1, ap.getAmount());
		assertEquals(AggregationPeriodUnit.YEAR, ap.getUnit());

		testRoundTrip(hapks.generateKey(APC_30MIN_MEL, LocalDateTime.now()));
		testRoundTrip(hapks.generateKey(APC_15MIN_MEL, LocalDateTime.now()));
		testRoundTrip(hapks.generateKey(APC_20MIN_MEL, LocalDateTime.now()));
		testRoundTrip(hapks.generateKey(APC_5MIN_MEL, LocalDateTime.now()));
		testRoundTrip(hapks.generateKey(APC_6MONTH_MEL, LocalDateTime.now()));
		testRoundTrip(hapks.generateKey(APC_5YEAR_MEL, LocalDateTime.now()));

	}

}
