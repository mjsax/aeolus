package de.hub.cs.dbis.aeolus.testUtils;

import static org.mockito.Mockito.mock;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class TestOutputCollectorTest {
	
	@Test
	public void testEmit() {
		TestOutputCollector collector = new TestOutputCollector();
		
		Values tuple = mock(Values.class);
		collector.emit(null, null, tuple);
		
		LinkedList<Values> result = new LinkedList<Values>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.output.get(null));
	}
	
	@Test
	public void testAck() {
		TestOutputCollector collector = new TestOutputCollector();
		
		Tuple tuple = mock(Tuple.class);
		collector.ack(tuple);
		
		LinkedList<Tuple> result = new LinkedList<Tuple>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.acked);
	}
	
	@Test
	public void testFail() {
		TestOutputCollector collector = new TestOutputCollector();
		
		Tuple tuple = mock(Tuple.class);
		collector.fail(tuple);
		
		LinkedList<Tuple> result = new LinkedList<Tuple>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.failed);
	}
	
}
