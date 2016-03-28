/**
 * 
 */
package net.sf.jabb.cjtsd;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author James Hu (Zhengmao Hu)
 *
 */
public class CJTSDTest {
	static ObjectMapper mapper = new ObjectMapper();
	
	static{
		mapper.setSerializationInclusion(Include.NON_NULL);
	}

	@Test
	public void testEmpty() throws JsonProcessingException {
		CJTSD cjtsd = CJTSD.builder().build();
		assertNotNull(cjtsd);
		assertEquals(0, cjtsd.getT().size());
		assertEquals(0, cjtsd.getD().size());
	}

	@Test
	public void testSameDuration(){
		LocalDateTime start = LocalDateTime.now();
		Duration duration = Duration.ofSeconds(500);
		CJTSD cjtsd = CJTSD.builder().setUnit(ChronoUnit.SECONDS)
				.add(start.plus(duration), duration)
				.addCount(10L)
				.add(start.plus(duration).plus(duration), duration)
				.addCount(20L)
				.add(start.plus(duration).plus(duration).plus(duration), duration)
				.addCount(30L)
				.build();
		assertEquals(3, cjtsd.getT().size());
		assertEquals(3, cjtsd.getC().size());
		assertEquals(1, cjtsd.getD().size());	// [500]
		assertEquals(500, cjtsd.getD().getInt(0));
		
		cjtsd = CJTSD.builder().setUnit(ChronoUnit.SECONDS)
				.add(start.plus(duration), duration)
				.addCount(10L)
				.add(start.plus(duration).plus(duration), duration)
				.addCount(20L)
				.add(start.plus(duration).plus(duration).plus(duration), duration)
				.addCount(30L)
				.add(start.plus(duration).plus(duration).plus(duration).plus(duration), Duration.ofSeconds(100))
				.addCount(40L)
				.add(start.plus(duration).plus(duration).plus(duration).plus(duration).plus(Duration.ofSeconds(100)), Duration.ofSeconds(100))
				.addCount(50L)
				.build();
		assertEquals(5, cjtsd.getT().size());
		assertEquals(5, cjtsd.getC().size());
		assertEquals(4, cjtsd.getD().size());	// [500, -1, -1, 100]
		assertEquals(500, cjtsd.getD().getInt(0));
		assertEquals(-1, cjtsd.getD().getInt(1));
		assertEquals(-1, cjtsd.getD().getInt(2));
		assertEquals(100, cjtsd.getD().getInt(3));

		duration = Duration.ofSeconds(99);
		cjtsd = CJTSD.builder().setUnit(ChronoUnit.SECONDS)
				.add(start.plus(duration), duration)
				.addCount(10L)
				.add(start.plus(duration).plus(duration), duration)
				.addCount(20L)
				.add(start.plus(duration).plus(duration).plus(duration), duration)
				.addCount(30L)
				.build();
		assertEquals(3, cjtsd.getT().size());
		assertEquals(3, cjtsd.getC().size());
		assertEquals(1, cjtsd.getD().size());	// [99]
		assertEquals(99, cjtsd.getD().getInt(0));

		cjtsd = CJTSD.builder().setUnit(ChronoUnit.SECONDS)
				.add(start.plus(duration), duration)
				.addCount(10L)
				.add(start.plus(duration).plus(duration), duration)
				.addCount(20L)
				.add(start.plus(duration).plus(duration).plus(duration), duration)
				.addCount(30L)
				.add(start.plus(duration).plus(duration).plus(duration), Duration.ofSeconds(100))
				.addCount(40L)
				.build();
		assertEquals(4, cjtsd.getT().size());
		assertEquals(4, cjtsd.getC().size());
		assertEquals(4, cjtsd.getD().size());	// [99, 99, 99, 100]
		assertEquals(99, cjtsd.getD().getInt(0));
		assertEquals(99, cjtsd.getD().getInt(1));
		assertEquals(99, cjtsd.getD().getInt(2));
		assertEquals(100, cjtsd.getD().getInt(3));
	}
}
