package de.hub.cs.dbis.aeolus.testUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class ForwardBoltTest {
	
	@Test
	public void testDeclareOutputFields() {
		Fields schema = new Fields("dummy");
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		
		ForwardBolt bolt = new ForwardBolt(schema);
		bolt.declareOutputFields(declarer);
		
		verify(declarer).declareStream(Utils.DEFAULT_STREAM_ID, schema);
		verifyNoMoreInteractions(declarer);
	}
	
	@Test
	public void testDeclareOutputFieldsMultipleStreams() {
		Fields schema = new Fields("dummy");
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		ForwardBolt bolt = new ForwardBolt(schema, streamIds);
		bolt.declareOutputFields(declarer);
		
		for(String stream : streamIds) {
			verify(declarer).declareStream(stream, schema);
		}
		verifyNoMoreInteractions(declarer);
	}
	
	@Test
	public void testExecute() {
		ForwardBolt bolt = new ForwardBolt(new Fields("dummy"));
		
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		LinkedList<Tuple> tuples = new LinkedList<Tuple>();
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 3; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(i);
			
			tuples.add(mock(Tuple.class));
			when(tuples.get(i).getValues()).thenReturn(attributes);
			result.add(attributes);
			
			bolt.execute(tuples.get(i));
			Assert.assertEquals(tuples, collector.acked);
		}
		
		Assert.assertEquals(result, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testExecuteMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		ForwardBolt bolt = new ForwardBolt(new Fields("dummy"), streamIds);
		
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		LinkedList<Tuple> tuples = new LinkedList<Tuple>();
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 3; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(i);
			
			tuples.add(mock(Tuple.class));
			when(tuples.get(i).getValues()).thenReturn(attributes);
			result.add(attributes);
			
			bolt.execute(tuples.get(i));
			Assert.assertEquals(tuples, collector.acked);
		}
		
		for(String stream : streamIds) {
			Assert.assertEquals(result, collector.output.get(stream));
		}
	}
}
