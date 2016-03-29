/**
 * 
 */
package net.sf.jabb.cjtsd;

import static org.junit.Assert.*;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

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
	
	@Test
	public void testEmpty() throws JsonProcessingException {
		CJTSD cjtsd = CJTSD.builder().build();
		assertNotNull(cjtsd);
		assertEquals(0, cjtsd.getT().size());
		assertEquals(0, cjtsd.getD().size());
	}

	@Test
	public void testSameDuration() throws IOException{
		LocalDateTime start = LocalDateTime.now();
		Duration duration = Duration.ofMillis(500);
		CJTSD cjtsd = CJTSD.builder().setUnit(ChronoUnit.MILLIS)
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
		assertEquals(500, cjtsd.getD().get(0).intValue());
		
		List<CJTSD.Entry> list = mapper.readValue(mapper.writeValueAsString(cjtsd), CJTSD.class).toList();
		assertEquals(3, list.size());
		assertEquals(10, list.get(0).getCount().longValue());
		assertEquals(20, list.get(1).getCount().longValue());
		assertEquals(30, list.get(2).getCount().longValue());
		assertEquals(Duration.ofMillis(500), list.get(0).getDuration());
		assertEquals(Duration.ofMillis(500), list.get(1).getDuration());
		assertEquals(Duration.ofMillis(500), list.get(2).getDuration());

		duration = Duration.ofSeconds(500);
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
		assertEquals(500, cjtsd.getD().get(0).intValue());
		assertEquals(-1, cjtsd.getD().get(1).intValue());
		assertEquals(-1, cjtsd.getD().get(2).intValue());
		assertEquals(100, cjtsd.getD().get(3).intValue());

		list = mapper.readValue(mapper.writeValueAsString(cjtsd), CJTSD.class).toList();
		assertEquals(5, list.size());
		assertEquals(10, list.get(0).getCount().longValue());
		assertEquals(20, list.get(1).getCount().longValue());
		assertEquals(30, list.get(2).getCount().longValue());
		assertEquals(40, list.get(3).getCount().longValue());
		assertEquals(50, list.get(4).getCount().longValue());
		assertEquals(Duration.ofSeconds(500), list.get(0).getDuration());
		assertEquals(Duration.ofSeconds(500), list.get(1).getDuration());
		assertEquals(Duration.ofSeconds(500), list.get(2).getDuration());
		assertEquals(Duration.ofSeconds(100), list.get(3).getDuration());
		assertEquals(Duration.ofSeconds(100), list.get(4).getDuration());

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
		assertEquals(99, cjtsd.getD().get(0).intValue());

		list = mapper.readValue(mapper.writeValueAsString(cjtsd), CJTSD.class).toList();
		assertEquals(3, list.size());
		assertEquals(Duration.ofSeconds(99), list.get(0).getDuration());
		assertEquals(Duration.ofSeconds(99), list.get(1).getDuration());
		assertEquals(Duration.ofSeconds(99), list.get(2).getDuration());

		duration = Duration.ofMinutes(99);
		cjtsd = CJTSD.builder().setUnit(ChronoUnit.MINUTES)
				.add(start.plus(duration), duration)
				.addCount(10L)
				.add(start.plus(duration).plus(duration), duration)
				.addCount(20L)
				.add(start.plus(duration).plus(duration).plus(duration), duration)
				.addCount(30L)
				.add(start.plus(duration).plus(duration).plus(duration), Duration.ofMinutes(100))
				.addCount(40L)
				.build();
		assertEquals(4, cjtsd.getT().size());
		assertEquals(4, cjtsd.getC().size());
		assertEquals(4, cjtsd.getD().size());	// [99, 99, 99, 100]
		assertEquals(99, cjtsd.getD().get(0).intValue());
		assertEquals(99, cjtsd.getD().get(1).intValue());
		assertEquals(99, cjtsd.getD().get(2).intValue());
		assertEquals(100, cjtsd.getD().get(3).intValue());
		
		list = mapper.readValue(mapper.writeValueAsString(cjtsd), CJTSD.class).toList();
		assertEquals(4, list.size());
		assertEquals(Duration.ofMinutes(99), list.get(0).getDuration());
		assertEquals(Duration.ofMinutes(99), list.get(1).getDuration());
		assertEquals(Duration.ofMinutes(99), list.get(2).getDuration());
		assertEquals(Duration.ofMinutes(100), list.get(3).getDuration());
	}
}
