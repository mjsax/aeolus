package de.hub.cs.dbis.aeolus.queries.utils;

import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestSpoutOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class OrderedInputSpoutTest {
	
	private class TestOrderedInputSpout extends AbstractOrderedInputSpout<String> {
		private static final long serialVersionUID = -6722924299495546729L;
		
		private final List<Deque<String>> data;
		private final Random rr;
		
		Map<Values, List<Integer>> emitted;
		
		TestOrderedInputSpout(List<Deque<String>> data, Random rr) {
			this.data = data;
			this.rr = rr;
		}
		
		public TestOrderedInputSpout(List<Deque<String>> data, Random rr, String streamID) {
			super(streamID);
			this.data = data;
			this.rr = rr;
		}
		
		
		@Override
		public void nextTuple() {
			int index = this.rr.nextInt(this.data.size());
			int old = index;
			while(this.data.get(index).size() == 0) {
				index = (index + 1) % this.data.size();
				if(index == old) {
					this.emitted = super.emitNextTuple(null, null, null);
					return;
				}
			}
			String line = this.data.get(index).removeFirst();
			this.emitted = super.emitNextTuple(new Integer(index), new Long(Long.parseLong(line.trim())), line);
		}
		
		@Override
		public void close() {}
		
		@Override
		public void activate() {}
		
		@Override
		public void deactivate() {}
		
		@Override
		public void ack(Object msgId) {}
		
		@Override
		public void fail(Object msgId) {}
		
		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
		
	}
	
	
	
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	
	
	@Test
	public void testDeclareOutputFields() {
		List<Deque<String>> data = new LinkedList<Deque<String>>();
		// test without explicit stream ID
		AbstractOrderedInputSpout<?> spout = new TestOrderedInputSpout(data, this.r);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(1, declarer.direct.size());
		Assert.assertEquals(1, declarer.schema.size());
		Assert.assertEquals(1, declarer.streamId.size());
		
		Assert.assertFalse(declarer.direct.get(0).booleanValue());
		Assert.assertEquals(2, declarer.schema.get(0).size());
		Assert.assertEquals("ts", declarer.schema.get(0).get(0));
		Assert.assertEquals("rawTuple", declarer.schema.get(0).get(1));
		Assert.assertNull(declarer.streamId.get(0));
		
		// test with explicit stream ID
		String streamID = "streamID";
		spout = new TestOrderedInputSpout(data, this.r, streamID);
		
		declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(1, declarer.direct.size());
		Assert.assertEquals(1, declarer.schema.size());
		Assert.assertEquals(1, declarer.streamId.size());
		
		Assert.assertFalse(declarer.direct.get(0).booleanValue());
		Assert.assertEquals(2, declarer.schema.get(0).size());
		Assert.assertEquals("ts", declarer.schema.get(0).get(0));
		Assert.assertEquals("rawTuple", declarer.schema.get(0).get(1));
		Assert.assertEquals(streamID, declarer.streamId.get(0));
	}
	
	@Test
	public void testClosePartiton() {
		Deque<String> partition1 = new LinkedList<String>();
		partition1.add("0");
		Deque<String> partition2 = new LinkedList<String>();
		partition2.add("1");
		@SuppressWarnings("unchecked")
		List<Deque<String>> data = new LinkedList<Deque<String>>(Arrays.asList(partition1, partition2));
		TestOrderedInputSpout spout = new TestOrderedInputSpout(data, this.r);
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put(TestOrderedInputSpout.NUMBER_OF_PARTITIONS, new Integer(2));
		spout.open(map, null, mock(SpoutOutputCollector.class));
		
		spout.nextTuple();
		Assert.assertEquals(0, spout.emitted.size());
		
		spout.nextTuple();
		Assert.assertEquals(1, spout.emitted.size());
		int partition = ((Long)spout.emitted.keySet().iterator().next().get(0)).intValue();
		spout.emitted.clear();
		
		spout.nextTuple();
		Assert.assertEquals(0, spout.emitted.size());
		
		Assert.assertFalse(spout.closePartition(new Integer(1 - partition)));
		Assert.assertTrue(spout.closePartition(new Integer(partition)));
		
		spout.nextTuple();
		Assert.assertEquals(1, spout.emitted.size());
		
		spout.nextTuple();
		Assert.assertEquals(0, spout.emitted.size());
		
		Assert.assertTrue(spout.closePartition(new Integer(1 - partition)));
		
		spout.nextTuple();
		Assert.assertEquals(0, spout.emitted.size());
	}
	
	@Test
	public void testSingleEmptyPartition() {
		@SuppressWarnings("unchecked")
		List<Deque<String>> data = new LinkedList<Deque<String>>(Arrays.asList(new LinkedList<String>()));
		TestOrderedInputSpout spout = new TestOrderedInputSpout(data, this.r);
		
		Config conf = new Config();
		conf.put(TestOrderedInputSpout.NUMBER_OF_PARTITIONS, new Integer(1));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(0, col.output.size());
	}
	
	@Test
	public void testAllPartitionsEmpty() {
		@SuppressWarnings("unchecked")
		List<Deque<String>> data = new LinkedList<Deque<String>>(Arrays.asList(new LinkedList<String>(),
			new LinkedList<String>(), new LinkedList<String>()));
		TestOrderedInputSpout spout = new TestOrderedInputSpout(data, this.r);
		
		Config conf = new Config();
		conf.put(TestOrderedInputSpout.NUMBER_OF_PARTITIONS, new Integer(3));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(0, col.output.size());
	}
	
	@Test
	public void testSinglePartition() {
		LinkedList<String> partition = new LinkedList<String>();
		partition.add("1");
		partition.add("2");
		partition.add("3");
		LinkedList<List<Object>> expectedResult = new LinkedList<List<Object>>();
		expectedResult.add(Arrays.asList(new Object[] {new Long(1), new String("1")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(2), new String("2")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(3), new String("3")}));
		@SuppressWarnings("unchecked")
		List<Deque<String>> data = new LinkedList<Deque<String>>(Arrays.asList(partition));
		
		TestOrderedInputSpout spout = new TestOrderedInputSpout(data, this.r);
		
		Config conf = new Config();
		conf.put(TestOrderedInputSpout.NUMBER_OF_PARTITIONS, new Integer(1));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(1, col.output.size());
		Assert.assertNotEquals(null, col.output.get(Utils.DEFAULT_STREAM_ID));
		Assert.assertEquals(expectedResult, col.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testMultiplePartitionsStrict() {
		Deque<String> partition1 = new LinkedList<String>();
		partition1.add("1");
		partition1.add("2");
		partition1.add("3");
		Deque<String> partition2 = new LinkedList<String>();
		partition2.add("1 ");
		partition2.add("2 ");
		partition2.add("3 ");
		Deque<String> partition3 = new LinkedList<String>();
		partition3.add(" 1");
		partition3.add(" 2");
		partition3.add(" 3");
		
		LinkedList<List<Object>> expectedResult = new LinkedList<List<Object>>();
		expectedResult.add(Arrays.asList(new Object[] {new Long(1), new String("1")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(1), new String("1 ")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(1), new String(" 1")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(2), new String("2 ")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(2), new String(" 2")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(2), new String("2")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(3), new String(" 3")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(3), new String("3")}));
		expectedResult.add(Arrays.asList(new Object[] {new Long(3), new String("3 ")}));
		Collections.sort(expectedResult, new Comp());
		@SuppressWarnings("unchecked")
		List<Deque<String>> data = new LinkedList<Deque<String>>(Arrays.asList(partition1, partition2, partition3));
		
		TestOrderedInputSpout spout = new TestOrderedInputSpout(data, this.r);
		
		Config conf = new Config();
		conf.put(TestOrderedInputSpout.NUMBER_OF_PARTITIONS, new Integer(3));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		spout.nextTuple();
		
		Assert.assertEquals(1, col.output.size());
		Assert.assertNotEquals(null, col.output.get(Utils.DEFAULT_STREAM_ID));
		Assert.assertEquals(9, col.output.get(Utils.DEFAULT_STREAM_ID).size());
		
		for(int i = 0; i < 3; ++i) {
			Set<List<Object>> expectedSubset = new HashSet<List<Object>>();
			Set<List<Object>> resultSubset = new HashSet<List<Object>>();
			for(int j = 0; j < 3; ++j) {
				expectedSubset.add(expectedResult.removeFirst());
				resultSubset.add(col.output.get(Utils.DEFAULT_STREAM_ID).removeFirst());
			}
			Assert.assertEquals(expectedSubset, resultSubset);
		}
	}
	
	@Test
	public void testMultiplePartitionsRandom() {
		LinkedList<List<Object>> expectedResult = new LinkedList<List<Object>>();
		
		int size, number, totalInputSize = 0;
		
		final int stepSizeRange = 1 + this.r.nextInt(6);
		
		size = 20 + this.r.nextInt(200);
		totalInputSize += size;
		Deque<String> partition1 = new LinkedList<String>();
		number = 0;
		for(int i = 0; i < size; ++i) {
			number += this.r.nextInt(stepSizeRange);
			partition1.add("" + number);
			expectedResult.add(Arrays.asList(new Object[] {new Long(number), new String("" + number)}));
		}
		
		size = 20 + this.r.nextInt(200);
		totalInputSize += size;
		Deque<String> partition2 = new LinkedList<String>();
		number = 0;
		for(int i = 0; i < size; ++i) {
			number += this.r.nextInt(stepSizeRange);
			partition2.add(" " + number);
			expectedResult.add(Arrays.asList(new Object[] {new Long(number), new String(" " + number)}));
		}
		
		size = 20 + this.r.nextInt(200);
		totalInputSize += size;
		Deque<String> partition3 = new LinkedList<String>();
		number = 0;
		for(int i = 0; i < size; ++i) {
			number += this.r.nextInt(stepSizeRange);
			partition3.add(number + " ");
			expectedResult.add(Arrays.asList(new Object[] {new Long(number), new String(number + " ")}));
		}
		Collections.sort(expectedResult, new Comp());
		
		@SuppressWarnings("unchecked")
		List<Deque<String>> data = new LinkedList<Deque<String>>(Arrays.asList(partition1, partition2, partition3));
		
		TestOrderedInputSpout spout = new TestOrderedInputSpout(data, this.r);
		
		Config conf = new Config();
		conf.put(TestOrderedInputSpout.NUMBER_OF_PARTITIONS, new Integer(3));
		
		TestSpoutOutputCollector col = new TestSpoutOutputCollector();
		spout.open(conf, mock(TopologyContext.class), new SpoutOutputCollector(col));
		
		final int numberOfNextTupleCalls = (int)(0.8 * totalInputSize) + this.r.nextInt((int)(0.4 * totalInputSize));
		for(int i = 0; i < numberOfNextTupleCalls; ++i) {
			spout.nextTuple();
		}
		
		Assert.assertEquals(1, col.output.size());
		Assert.assertNotEquals(null, col.output.get(Utils.DEFAULT_STREAM_ID));
		
		System.out.println(expectedResult);
		System.out.println(col.output.get(Utils.DEFAULT_STREAM_ID));
		
		List<Object> lastRemoved = null;
		while(expectedResult.size() > col.output.get(Utils.DEFAULT_STREAM_ID).size()) {
			lastRemoved = expectedResult.removeLast();
		}
		if(lastRemoved != null) {
			while(expectedResult.size() > 0
				&& ((Long)lastRemoved.get(0)).longValue() == ((Long)expectedResult.getLast().get(0)).longValue()) {
				expectedResult.removeLast();
			}
		}
		
		while(expectedResult.size() > 0) {
			Set<List<Object>> expectedSubset = new HashSet<List<Object>>();
			Set<List<Object>> resultSubset = new HashSet<List<Object>>();
			List<Object> first;
			do {
				first = expectedResult.removeFirst();
				expectedSubset.add(first);
				resultSubset.add(col.output.get(Utils.DEFAULT_STREAM_ID).removeFirst());
				
				if(expectedResult.size() == 0) {
					break;
				}
			} while(((Long)expectedResult.getFirst().get(0)).longValue() == ((Long)first.get(0)).longValue());
			
			Assert.assertEquals(expectedSubset, resultSubset);
		}
	}
}
