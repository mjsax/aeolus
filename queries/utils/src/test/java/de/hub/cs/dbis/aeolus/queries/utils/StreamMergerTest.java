package de.hub.cs.dbis.aeolus.queries.utils;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class StreamMergerTest {
	@Mock private GeneralTopologyContext contextMock;
	
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	
	
	@Test
	public void testEmpty() {
		@SuppressWarnings({"unchecked", "rawtypes"})
		StreamMerger merger = new StreamMerger(Arrays.asList(new Integer(0)), 0);
		
		Assert.assertNull(merger.getNextTuple());
	}
	
	@Test
	public void testTuple() {
		StreamMerger<Tuple> merger = new StreamMerger<Tuple>(Arrays.asList(0), "ts");
		
		when(this.contextMock.getComponentId(0)).thenReturn("bolt1");
		when(this.contextMock.getComponentId(1)).thenReturn("bolt2");
		when(this.contextMock.getComponentId(2)).thenReturn("bolt3");
		when(this.contextMock.getComponentOutputFields(eq("bolt1"), anyString())).thenReturn(new Fields("ts"));
		when(this.contextMock.getComponentOutputFields(eq("bolt2"), anyString()))
			.thenReturn(new Fields("x", "ts", "y"));
		when(this.contextMock.getComponentOutputFields(eq("bolt3"), anyString()))
			.thenReturn(new Fields("a", "b", "ts"));
		
		Tuple t = new TupleImpl(this.contextMock, new Values(new Long(0)), 0, null);
		merger.addTuple(0, t);
		
		Assert.assertSame(t, merger.getNextTuple());
		Assert.assertNull(merger.getNextTuple());
		
		Tuple t2 = new TupleImpl(this.contextMock, new Values(mock(Object.class), new Long(0), mock(Object.class)), 1,
			null);
		merger.addTuple(0, t2);
		t = new TupleImpl(this.contextMock, new Values(mock(Object.class), mock(Object.class), (long)0), 2, null);
		merger.addTuple(0, t);
		
		Assert.assertSame(t2, merger.getNextTuple());
		Assert.assertSame(t, merger.getNextTuple());
		Assert.assertNull(merger.getNextTuple());
	}
	
	@Test
	public void testValues() {
		StreamMerger<Values> merger = new StreamMerger<Values>(Arrays.asList(0), 0);
		
		Values t = new Values(new Long(0));
		merger.addTuple(0, t);
		
		Assert.assertSame(t, merger.getNextTuple());
		Assert.assertNull(merger.getNextTuple());
		
		Values t2 = new Values(new Long(0), mock(Object.class), mock(Object.class));
		merger.addTuple(0, t2);
		t = new Values((long)0, mock(Object.class), mock(Object.class));
		merger.addTuple(0, t);
		
		Assert.assertSame(t2, merger.getNextTuple());
		Assert.assertSame(t, merger.getNextTuple());
		Assert.assertNull(merger.getNextTuple());
	}
	
	@Test
	public void testMultiplePartitions() {
		StreamMerger<Values> merger = new StreamMerger<Values>(Arrays.asList(0, 1), 0);
		
		Values t1 = new Values(new Long(0));
		merger.addTuple(0, t1);
		
		Assert.assertNull(merger.getNextTuple());
		
		Values t2 = new Values(new Long(0), mock(Object.class), mock(Object.class));
		merger.addTuple(1, t2);
		Values res = merger.getNextTuple();
		Assert.assertTrue(res.equals(t1) || res.equals(t2));
		if(res.equals(t1)) {
			res = merger.getNextTuple();
			Assert.assertSame(t2, res);
		} else {
			res = merger.getNextTuple();
			Assert.assertSame(t1, res);
			
		}
		
		t1 = new Values((long)0);
		merger.addTuple(this.r.nextInt(1), t1);
		Assert.assertSame(t1, merger.getNextTuple());
		
		t1 = new Values((long)1);
		merger.addTuple(1, t1);
		Assert.assertNull(merger.getNextTuple());
		
		t2 = new Values((long)2);
		merger.addTuple(0, t2);
		Assert.assertSame(t1, merger.getNextTuple());
		Assert.assertNull(merger.getNextTuple());
		
		t1 = new Values((long)2);
		merger.addTuple(0, t1);
		Assert.assertNull(merger.getNextTuple());
		
		Values t3 = new Values(new Long(2));
		merger.addTuple(1, t3);
		Values t4 = new Values(new Long(3));
		merger.addTuple(0, t4);
		Values t5 = new Values(new Long(3));
		merger.addTuple(1, t5);
		
		res = merger.getNextTuple();
		Assert.assertTrue(res.equals(t2) || res.equals(t3));
		if(res.equals(t2)) {
			res = merger.getNextTuple();
			Assert.assertTrue(res.equals(t1) || res.equals(t3));
			if(res.equals(t1)) {
				Assert.assertSame(t3, merger.getNextTuple());
			} else {
				Assert.assertSame(t1, merger.getNextTuple());
			}
		} else {
			Assert.assertSame(t2, merger.getNextTuple());
			Assert.assertSame(t1, merger.getNextTuple());
		}
		res = merger.getNextTuple();
		Assert.assertTrue(res.equals(t4) || res.equals(t5));
		if(res.equals(t4)) {
			Assert.assertSame(t5, merger.getNextTuple());
		} else {
			Assert.assertSame(t4, merger.getNextTuple());
		}
	}
	
	@Test
	public void testRandomValues() {
		final int numberOfPartitions = 2 + this.r.nextInt(8);
		double duplicatesFraction = this.r.nextDouble();
		if(this.r.nextBoolean()) {
			duplicatesFraction = 0;
		}
		final int numberOfTuples = numberOfPartitions * 10
			+ this.r.nextInt(numberOfPartitions * (1 + this.r.nextInt(10)));
		
		ArrayList<Integer> partitionIds = new ArrayList<Integer>(numberOfPartitions);
		int[] currentTs = new int[numberOfPartitions];
		for(int i = 0; i < numberOfPartitions; ++i) {
			partitionIds.add(i);
			currentTs[i] = 1;
		}
		StreamMerger<Values> merger = new StreamMerger<Values>(partitionIds, 0);
		
		
		LinkedList<Values> expectedResult = new LinkedList<Values>();
		LinkedList<Values> result = new LinkedList<Values>();
		
		
		int counter = 0;
		
		while(counter < numberOfTuples) {
			int inserts = this.r.nextInt(2 * numberOfPartitions);
			
			for(int i = 0; i < inserts; ++i) {
				int partitionId = partitionIds.get(this.r.nextInt(numberOfPartitions));
				
				Values t = new Values();
				Long ts = (long)currentTs[partitionId] - 1;
				t.add(ts);
				for(int j = 0; j < 9; ++j) {
					t.add((char)(32 + this.r.nextInt(95)));
				}
				expectedResult.add(t);
				merger.addTuple(partitionId, t);
				
				if(++counter == numberOfTuples) {
					break;
				}
				
				if(1 - this.r.nextDouble() > duplicatesFraction) {
					++currentTs[partitionId];
				}
			}
			
			int reads = this.r.nextInt(2 * numberOfPartitions);
			for(int i = 0; i < reads; ++i) {
				Values res = merger.getNextTuple();
				if(res != null) {
					result.add(res);
				}
			}
			
		}
		
		Collections.sort(expectedResult, new Comp());
		
		List<Object> lastRemoved = null;
		while(expectedResult.size() > result.size()) {
			lastRemoved = expectedResult.removeLast();
		}
		if(lastRemoved != null) {
			while(expectedResult.size() > 0 && ((Long)lastRemoved.get(0)) == ((Long)expectedResult.getLast().get(0))) {
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
				resultSubset.add(result.removeFirst());
				
				if(expectedResult.size() == 0) {
					break;
				}
			} while(((Long)expectedResult.getFirst().get(0)) == ((Long)first.get(0)));
			
			Assert.assertEquals(expectedSubset, resultSubset);
		}
		
	}
	
}
