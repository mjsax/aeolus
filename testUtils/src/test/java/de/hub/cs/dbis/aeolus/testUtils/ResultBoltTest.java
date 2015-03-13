package de.hub.cs.dbis.aeolus.testUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class ResultBoltTest {
	
	@Test
	public void testDeclareOutputFields() {
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		
		ResultBolt bolt = new ResultBolt();
		bolt.declareOutputFields(declarer);
		
		verifyZeroInteractions(declarer);
	}
	
	
	@Test
	public void testExecute() {
		ResultBolt bolt = new ResultBolt();
		
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		LinkedList<Tuple> tuples = new LinkedList<Tuple>();
		
		for(int i = 0; i < 10; ++i) {
			Values attributes = new Values();
			attributes.add(i);
			
			tuples.add(mock(Tuple.class));
			when(tuples.get(i).getValues()).thenReturn(attributes);
			
			bolt.execute(tuples.get(i));
			Assert.assertEquals(tuples, collector.acked);
		}
		
		Assert.assertEquals(tuples, collector.acked);
		Assert.assertEquals(0, collector.failed.size());
		Assert.assertEquals(0, collector.output.size());
	}
	
}
