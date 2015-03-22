/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package de.hub.cs.dbis.aeolus.batching;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(StormConnector.class)
public class AbstractBatchCollectorTest {
	private Random r;
	final String sourceId = "sourceId";
	
	
	private int[] generateConsumerTasks(int maxNumberOfReceivers, int maxNumberOfTasks) {
		assert (maxNumberOfReceivers > 0);
		assert (maxNumberOfTasks > 0);
		
		int minNumberOfReceiverd = 1;
		int minNumberOfTasks = 1;
		if(maxNumberOfReceivers > 1) {
			++minNumberOfReceiverd;
			--maxNumberOfReceivers;
		} else if(maxNumberOfTasks > 1) {
			++minNumberOfTasks;
			--maxNumberOfTasks;
		}
		
		final int numberOfConsumers = minNumberOfReceiverd + this.r.nextInt(maxNumberOfReceivers);
		final int[] numberOfConsumerTasks = new int[numberOfConsumers];
		for(int i = 0; i < numberOfConsumers; ++i) {
			numberOfConsumerTasks[i] = minNumberOfTasks + this.r.nextInt(maxNumberOfTasks);
		}
		
		return numberOfConsumerTasks;
	}
	
	private String[] generateStreamIds(final int numberOfStreams, int mode) {
		assert (numberOfStreams > 0);
		assert (0 <= mode && mode <= 2);
		
		String[] outputStreams = new String[numberOfStreams];
		for(int i = 0; i < outputStreams.length; ++i) {
			switch(mode) {
			case 0:
				outputStreams[i] = Utils.DEFAULT_STREAM_ID;
				break;
			case 1:
				outputStreams[i] = "streamId-" + i;
				break;
			case 2:
				outputStreams[i] = "streamId-" + this.r.nextInt(outputStreams.length);
				break;
			}
		}
		return outputStreams;
	}
	
	
	
	@Before
	public void prepareTest() {
		final long seed = System.currentTimeMillis();
		this.r = new Random(seed);
		System.out.println("Test seed: " + seed);
		
		PowerMockito.mockStatic(StormConnector.class);
	}
	
	
	
	@Test
	public void testEmitShuffleSimple() {
		this.runTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(1, 1));
	}
	
	@Test
	public void testEmitShuffleMultipleConsumerTasks() {
		this.runTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(1, 10));
	}
	
	@Test
	public void testEmitShuffleMultipleConsumersNoDop() {
		this.runTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(10, 1));
		
	}
	
	@Test
	public void testEmitShuffleFull() {
		this.runTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(5, 5));
	}
	
	private void runTestEmitShuffleDefaultOutputStream(int[] numberOfConsumerTasks) {
		this.runTestEmitShuffle(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 0));
	}
	
	@Test
	public void testEmitShuffleDistinctOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		this.runTestEmitShuffle(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 1));
	}
	
	@Test
	public void testEmitShuffleRandomOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		this.runTestEmitShuffle(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 2));
	}
	
	private void runTestEmitShuffle(int[] numberOfConsumerTasks, String[] outputStreams) {
		assert (numberOfConsumerTasks.length == outputStreams.length);
		
		final int batchSize = 1 + this.r.nextInt(5);
		
		final String consumerPrefix = "consumer-";
		final int[] numberOfAttributes = new int[numberOfConsumerTasks.length];
		
		@SuppressWarnings("unchecked")
		List<Batch>[] expectedResult = new List[numberOfConsumerTasks.length];
		Batch[] currentBatch = new Batch[numberOfConsumerTasks.length];
		
		Grouping shuffle = mock(Grouping.class);
		when(new Boolean(shuffle.is_set_fields())).thenReturn(new Boolean(false));
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(this.sourceId);
		
		Map<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		assert (numberOfConsumerTasks.length < 100);
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			assert (numberOfConsumerTasks[i] < 100);
			final String consumerId = consumerPrefix + i;
			
			Map<String, Grouping> consumer = new HashMap<String, Grouping>();
			consumer.put(consumerId, shuffle);
			
			final List<Integer> consumerTasks = new ArrayList<Integer>();
			for(int j = 0; j < numberOfConsumerTasks[i]; ++j) {
				consumerTasks.add(new Integer(i * 100 + j));
			}
			when(context.getComponentTasks(consumerId)).thenReturn(consumerTasks);
			
			Map<String, Grouping> streamMapping = targets.get(outputStreams[i]);
			if(streamMapping == null) {
				targets.put(outputStreams[i], consumer);
			} else {
				streamMapping.putAll(consumer);
			}
			
			Fields schema = context.getComponentOutputFields(this.sourceId, outputStreams[i]);
			if(schema == null) {
				numberOfAttributes[i] = 1 + this.r.nextInt(5);
				String[] attributes = new String[numberOfAttributes[i]];
				for(int j = 0; j < numberOfAttributes[i]; ++j) {
					attributes[j] = "a" + i + "_" + j;
				}
				when(context.getComponentOutputFields(this.sourceId, outputStreams[i])).thenReturn(
					new Fields(attributes));
			} else {
				numberOfAttributes[i] = schema.size();
			}
			
			expectedResult[i] = new LinkedList<Batch>();
			currentBatch[i] = new Batch(batchSize, numberOfAttributes[i]);
		}
		when(context.getThisTargets()).thenReturn(targets);
		
		
		
		TestBatchCollector collector = new TestBatchCollector(context, batchSize);
		
		final int numberOfTuples = batchSize * 20 + this.r.nextInt(batchSize * 10);
		for(int i = 0; i < numberOfTuples; ++i) {
			final int index = this.r.nextInt(numberOfConsumerTasks.length);
			
			Values tuple = new Values();
			for(int j = 0; j < numberOfAttributes[index]; ++j) {
				switch(this.r.nextInt(3)) {
				case 0:
					tuple.add(new Integer(this.r.nextInt()));
					break;
				case 1:
					tuple.add(new Double(this.r.nextInt() + this.r.nextDouble()));
					break;
				default:
					tuple.add(new String("" + (char)(32 + this.r.nextInt(95))));
					break;
				}
				
			}
			
			String outputStream = outputStreams[index];
			if(outputStream.equals(Utils.DEFAULT_STREAM_ID)) {
				collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, tuple, null);
			} else {
				collector.tupleEmit(outputStream, null, tuple, null);
			}
			
			for(int j = 0; j < numberOfConsumerTasks.length; ++j) {
				if(outputStreams[index].equals(outputStreams[j])) {
					currentBatch[j].addTuple(tuple);
					if(currentBatch[j].isFull()) {
						expectedResult[j].add(currentBatch[j]);
						currentBatch[j] = new Batch(batchSize, numberOfAttributes[j]);
					}
					
				}
			}
		}
		
		
		
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			Assert.assertEquals(expectedResult[i], collector.batchBuffer.get(outputStreams[i]));
		}
	}
	
	@Test
	public void testEmitFieldGroupingSimple() {
		this.runTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(1, 1));
	}
	
	@Test
	public void testEmitFieldGroupingMultipleConsumerTasks() {
		this.runTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(1, 10));
	}
	
	@Test
	public void testEmitFieldGroupingMultipleConsumersNoDop() {
		this.runTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(10, 1));
	}
	
	@Test
	public void testEmitFieldGroupingFull() {
		this.runTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(3, 3));
	}
	
	private void runTestEmitFieldsGroupingDefaultOutputStream(int[] numberOfConsumerTasks) {
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 0),
			false);
	}
	
	@Test
	public void testEmitShuffeFieldGroupingCombined() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 0),
			true);
	}
	
	@Test
	public void testEmitFieldGroupingDistinctOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 1),
			false);
	}
	
	@Test
	public void testEmitFieldGroupingRandomOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 2),
			false);
	}
	
	private void runTestEmitFieldsGrouping(int[] numberOfConsumerTasks, String[] outputStreams, boolean mixedGrouping) {
		assert (numberOfConsumerTasks.length == outputStreams.length);
		assert (numberOfConsumerTasks.length < 100);
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			assert (numberOfConsumerTasks[i] < 100);
		}
		
		final int batchSize = 1 + this.r.nextInt(5);
		final String consumerPrefix = "consumer-";
		
		Map<String, Map<Set<Integer>, Batch>> currentBatch = new HashMap<String, Map<Set<Integer>, Batch>>();
		Map<String, List<Integer>> taskIds = new HashMap<String, List<Integer>>();
		Map<String, List<String>> streams = new HashMap<String, List<String>>();
		
		Grouping grouping = mock(Grouping.class);
		boolean[] groupings = new boolean[numberOfConsumerTasks.length];
		if(mixedGrouping) {
			assert (numberOfConsumerTasks.length > 1);
			boolean shuff = false;
			boolean fields = false;
			while(!shuff || !fields) {
				for(int i = 0; i < groupings.length; ++i) {
					groupings[i] = this.r.nextBoolean();
				}
				shuff = false;
				fields = false;
				for(int i = 0; i < groupings.length; ++i) {
					if(groupings[i] == true) {
						fields = true;
					} else {
						shuff = true;
					}
				}
			}
		} else {
			for(int i = 0; i < groupings.length; ++i) {
				groupings[i] = true;
			}
		}
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(this.sourceId);
		
		int maxNumberOfBatches = 1;
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			if(groupings[i]) {
				maxNumberOfBatches *= numberOfConsumerTasks[i];
			}
		}
		final int numberOfDistinctValues = 1 + maxNumberOfBatches / 2 + this.r.nextInt(2 * maxNumberOfBatches);
		Values[] partitionsTuple = new Values[numberOfDistinctValues];
		for(int i = 0; i < numberOfDistinctValues; ++i) {
			partitionsTuple[i] = new Values(new Integer(i));
		}
		
		
		Map<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			final String consumerId = consumerPrefix + i;
			
			Map<String, Grouping> consumer = new HashMap<String, Grouping>();
			consumer.put(consumerId, grouping);
			
			Map<Set<Integer>, Batch> old = currentBatch.get(outputStreams[i]);
			if(old == null) {
				old = new HashMap<Set<Integer>, Batch>();
			}
			currentBatch.put(outputStreams[i], new HashMap<Set<Integer>, Batch>());
			final List<Integer> consumerTasks = new ArrayList<Integer>();
			for(int j = 0; j < numberOfConsumerTasks[i]; ++j) {
				Integer tid = new Integer(i * 100 + j);
				consumerTasks.add(tid);
				
				if(groupings[i]) {
					if(old.size() == 0) {
						Set<Integer> s = new HashSet<Integer>();
						s.add(tid);
						currentBatch.get(outputStreams[i]).put(s, new Batch(batchSize, 1));
					} else {
						for(Set<Integer> s : old.keySet()) {
							Set<Integer> s2 = new HashSet<Integer>();
							s2.addAll(s);
							s2.add(tid);
							currentBatch.get(outputStreams[i]).put(s2, new Batch(batchSize, 1));
						}
					}
				} else {
					currentBatch.put(outputStreams[i], old);
				}
			}
			when(context.getComponentTasks(consumerId)).thenReturn(consumerTasks);
			taskIds.put(consumerId, consumerTasks);
			
			if(groupings[i]) {
				for(int j = 0; j < numberOfDistinctValues; ++j) {
					when(
						StormConnector.getFieldsGroupingReceiverTaskId(any(WorkerTopologyContext.class),
							eq(this.sourceId), eq(outputStreams[i]), eq(consumerId), eq(partitionsTuple[j])))
						.thenReturn(consumerTasks.get(j % numberOfConsumerTasks[i]));
				}
			}
			
			Map<String, Grouping> streamMapping = targets.get(outputStreams[i]);
			if(streamMapping == null) {
				targets.put(outputStreams[i], consumer);
			} else {
				streamMapping.putAll(consumer);
			}
			
			Fields schema = context.getComponentOutputFields(this.sourceId, outputStreams[i]);
			if(schema == null) {
				when(context.getComponentOutputFields(this.sourceId, outputStreams[i])).thenReturn(
					new Fields("attribute"));
			}
			
			List<String> receivers = streams.get(outputStreams[i]);
			if(receivers == null) {
				receivers = new LinkedList<String>();
				streams.put(outputStreams[i], receivers);
			}
			receivers.add(consumerId);
		}
		when(context.getThisTargets()).thenReturn(targets);
		
		if(mixedGrouping) {
			// this is kind of a hack:
			// the ordering of the returned values of .if_set_fields() must correspond to the call sequence in
			// AbstractBatchCollector
			// => thus, we emulate this call sequence using two for loops and mock accordingly
			// => this hack make the test depending on implementation details of AbstractBatchCollector what is of
			// course not desired... (however, it is the only way to make the test run)
			OngoingStubbing<Boolean> stub = when(new Boolean(grouping.is_set_fields()));
			
			for(Entry<String, Map<String, Grouping>> e : targets.entrySet()) {
				for(Entry<String, Grouping> e2 : e.getValue().entrySet()) {
					int j = 0;
					while(!e2.getKey().equals(consumerPrefix + j)) {
						++j;
					}
					stub = stub.thenReturn(new Boolean(groupings[j]));
					
				}
			}
		} else {
			when(new Boolean(grouping.is_set_fields())).thenReturn(new Boolean(true));
		}
		
		
		
		Map<String, List<Batch>> expectedResult = new HashMap<String, List<Batch>>();
		for(String s : streams.keySet()) {
			expectedResult.put(s, new LinkedList<Batch>());
		}
		
		TestBatchCollector collector = new TestBatchCollector(context, batchSize);
		
		final int numberOfTuples = numberOfDistinctValues * batchSize * 20
			+ this.r.nextInt(numberOfDistinctValues * batchSize * 10);
		for(int i = 0; i < numberOfTuples; ++i) {
			final int index = this.r.nextInt(numberOfConsumerTasks.length);
			
			Values tuple = new Values(new Integer(this.r.nextInt(numberOfDistinctValues)));
			String outputStream = outputStreams[index];
			if(outputStream.equals(Utils.DEFAULT_STREAM_ID)) {
				collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, tuple, null);
			} else {
				collector.tupleEmit(outputStream, null, tuple, null);
			}
			
			for(Entry<String, List<String>> s : streams.entrySet()) {
				if(s.getKey().equals(outputStream)) {
					Set<Integer> batchKey = new HashSet<Integer>();
					
					for(int k = 0; k < numberOfConsumerTasks.length; ++k) {
						if(outputStreams[k].equals(outputStream) && groupings[k]) {
							batchKey.add(taskIds.get(consumerPrefix + k).get(
								((Integer)tuple.get(0)).intValue() % numberOfConsumerTasks[k]));
						}
					}
					
					Batch batch = currentBatch.get(outputStream).get(batchKey);
					batch.addTuple(tuple);
					if(batch.isFull()) {
						expectedResult.get(outputStream).add(batch);
						currentBatch.get(outputStream).put(batchKey, new Batch(batchSize, 1));
					}
					
				}
			}
		}
		
		
		
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			Assert.assertEquals(expectedResult.get(outputStreams[i]), collector.batchBuffer.get(outputStreams[i]));
		}
	}
	
	@Test
	public void testFlush() {
		final String secondStream = "stream-2";
		final String thirdStream = "stream-3";
		
		HashMap<String, Grouping> consumer = new HashMap<String, Grouping>();
		consumer.put("receiver", mock(Grouping.class));
		
		Map<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		targets.put(Utils.DEFAULT_STREAM_ID, consumer);
		targets.put(secondStream, consumer);
		targets.put(thirdStream, consumer);
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisTargets()).thenReturn(targets);
		when(context.getComponentOutputFields(null, Utils.DEFAULT_STREAM_ID)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, secondStream)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, thirdStream)).thenReturn(new Fields("dummy"));
		
		final int batchSize = 5;
		TestBatchCollector collector = new TestBatchCollector(context, batchSize);
		
		final int numberOfTuples = 42;
		for(int i = 0; i < numberOfTuples - 2; ++i) {
			collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(i)), null);
			collector.tupleEmit(secondStream, null, new Values(new Integer(i)), null);
			collector.tupleEmit(thirdStream, null, new Values(new Integer(i)), null);
		}
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(40)), null);
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(41)), null);
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(42)), null);
		
		collector.tupleEmit(secondStream, null, new Values(new Integer(40)), null);
		collector.tupleEmit(secondStream, null, new Values(new Integer(41)), null);
		
		Assert.assertEquals(8, collector.batchBuffer.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(8, collector.batchBuffer.get(secondStream).size());
		Assert.assertEquals(8, collector.batchBuffer.get(thirdStream).size());
		
		collector.flush();
		
		Assert.assertEquals(9, collector.batchBuffer.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(9, collector.batchBuffer.get(secondStream).size());
		Assert.assertEquals(8, collector.batchBuffer.get(thirdStream).size());
	}
}
