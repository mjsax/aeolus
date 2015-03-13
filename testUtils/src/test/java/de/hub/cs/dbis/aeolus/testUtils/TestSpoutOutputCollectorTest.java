package de.hub.cs.dbis.aeolus.testUtils;

import static org.mockito.Mockito.mock;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Values;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class TestSpoutOutputCollectorTest {
	
	@Test
	public void testEmit() {
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		
		Values tuple = mock(Values.class);
		collector.emit(null, tuple, null);
		
		LinkedList<Values> result = new LinkedList<Values>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.output.get(null));
	}
	
}
