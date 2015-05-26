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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
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
	
	private HashMap<String, Integer> generateBatchSizes(String[] streamIds) {
		HashMap<String, Integer> batchSizes = new HashMap<String, Integer>();
		
		boolean foundZero;
		do {
			for(String id : streamIds) {
				batchSizes.put(id, new Integer(this.r.nextInt(6)));
			}
			
			foundZero = false;
			for(Integer bS : batchSizes.values()) {
				if(bS.intValue() == 0) {
					foundZero = true;
				}
			}
		} while(foundZero);
		
		for(Integer bS : batchSizes.values()) {
			assert (bS.intValue() > 0);
		}
		
		return batchSizes;
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
		this.prepateAndRunTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(1, 1));
	}
	
	@Test
	public void testEmitShuffleMultipleConsumerTasks() {
		this.prepateAndRunTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(1, 10));
	}
	
	@Test
	public void testEmitShuffleMultipleConsumersNoDop() {
		this.prepateAndRunTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(10, 1));
		
	}
	
	@Test
	public void testEmitShuffleFull() {
		this.prepateAndRunTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks(5, 5));
	}
	
	private void prepateAndRunTestEmitShuffleDefaultOutputStream(int[] numberOfConsumerTasks) {
		this.runTestEmitShuffle(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 0), null);
	}
	
	@Test
	public void testEmitShuffleDistinctOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		this.runTestEmitShuffle(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 1), null);
	}
	
	@Test
	public void testEmitShuffleRandomOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		this.runTestEmitShuffle(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 2), null);
	}
	
	@Test
	public void testEmitShuffleRandomOutputStreamsDifferentBatchSizes() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		String[] streamIds = this.generateStreamIds(numberOfConsumerTasks.length, 2);
		this.runTestEmitShuffle(numberOfConsumerTasks, streamIds, this.generateBatchSizes(streamIds));
	}
	
	private void runTestEmitShuffle(int[] numberOfConsumerTasks, String[] outputStreams, HashMap<String, Integer> batchSizes) {
		assert (numberOfConsumerTasks.length == outputStreams.length);
		
		final int batchSize = 1 + this.r.nextInt(5);
		
		final String consumerPrefix = "consumer-";
		final int[] numberOfAttributes = new int[numberOfConsumerTasks.length];
		
		@SuppressWarnings("unchecked")
		List<Object>[] expectedResult = new List[numberOfConsumerTasks.length];
		Batch[] currentBatch = new Batch[numberOfConsumerTasks.length];
		
		Grouping grouping = mock(Grouping.class);
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(this.sourceId);
		
		Map<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		assert (numberOfConsumerTasks.length < 100);
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			assert (numberOfConsumerTasks[i] < 100);
			final String consumerId = consumerPrefix + i;
			
			Map<String, Grouping> consumer = new HashMap<String, Grouping>();
			consumer.put(consumerId, grouping);
			
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
			
			expectedResult[i] = new LinkedList<Object>();
			if(batchSizes == null) {
				currentBatch[i] = new Batch(batchSize, numberOfAttributes[i]);
			} else {
				int bS = batchSizes.get(outputStreams[i]).intValue();
				if(bS > 0) {
					currentBatch[i] = new Batch(bS, numberOfAttributes[i]);
				}
			}
		}
		when(context.getThisTargets()).thenReturn(targets);
		
		
		
		TestBatchCollector collector;
		int maxBatchSize = 0;
		if(batchSizes == null) {
			collector = new TestBatchCollector(context, batchSize);
			maxBatchSize = batchSize;
		} else {
			collector = new TestBatchCollector(context, batchSizes);
			for(Integer b : batchSizes.values()) {
				int bS = b.intValue();
				if(bS > maxBatchSize) {
					maxBatchSize = bS;
				}
			}
		}
		
		final int numberOfTuples = maxBatchSize * 20 + this.r.nextInt(maxBatchSize * 10);
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
			
			collector.tupleEmit(outputStreams[index], null, tuple, null);
			
			for(int j = 0; j < numberOfConsumerTasks.length; ++j) {
				if(outputStreams[index].equals(outputStreams[j])) {
					if(currentBatch[j] != null) {
						currentBatch[j].addTuple(tuple);
						if(currentBatch[j].isFull()) {
							expectedResult[j].add(currentBatch[j]);
							if(batchSizes == null) {
								currentBatch[j] = new Batch(batchSize, numberOfAttributes[j]);
							} else {
								currentBatch[j] = new Batch(batchSizes.get(outputStreams[index]).intValue(),
									numberOfAttributes[j]);
							}
						}
					} else {
						expectedResult[j].add(tuple);
					}
				}
			}
		}
		
		
		
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			Assert.assertEquals(expectedResult[i], collector.resultBuffer.get(outputStreams[i]));
		}
	}
	
	@Test
	public void testEmitFieldGroupingSimple() {
		this.prepareAndRunTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(1, 1));
	}
	
	@Test
	public void testEmitFieldGroupingMultipleConsumerTasks() {
		this.prepareAndRunTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(1, 10));
	}
	
	@Test
	public void testEmitFieldGroupingMultipleConsumersNoDop() {
		this.prepareAndRunTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(10, 1));
	}
	
	@Test
	public void testEmitFieldGroupingFull() {
		this.prepareAndRunTestEmitFieldsGroupingDefaultOutputStream(this.generateConsumerTasks(3, 3));
	}
	
	private void prepareAndRunTestEmitFieldsGroupingDefaultOutputStream(int[] numberOfConsumerTasks) {
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 0),
			null);
	}
	
	@Test
	public void testEmitFieldGroupingDistinctOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 1),
			null);
	}
	
	@Test
	public void testEmitFieldGroupingRandomOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 2),
			null);
	}
	
	@Test
	public void testEmitFieldGroupingRandomOutputStreamsDifferentBatchSizes() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		String[] streamIds = this.generateStreamIds(numberOfConsumerTasks.length, 2);
		this.runTestEmitFieldsGrouping(numberOfConsumerTasks, streamIds, this.generateBatchSizes(streamIds));
	}
	
	private void runTestEmitFieldsGrouping(int[] numberOfConsumerTasks, String[] outputStreams, HashMap<String, Integer> batchSizes) {
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
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(this.sourceId);
		
		int maxNumberOfBatches = 1;
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			maxNumberOfBatches *= numberOfConsumerTasks[i];
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
			
			Map<Set<Integer>, Batch> old = currentBatch.put(outputStreams[i], new HashMap<Set<Integer>, Batch>());
			if(old == null) {
				old = new HashMap<Set<Integer>, Batch>();
			}
			
			final List<Integer> consumerTasks = new ArrayList<Integer>();
			for(int j = 0; j < numberOfConsumerTasks[i]; ++j) {
				Integer tid = new Integer(i * 100 + j);
				consumerTasks.add(tid);
				
				if(old.size() == 0) {
					Set<Integer> s = new HashSet<Integer>();
					s.add(tid);
					if(batchSizes == null) {
						currentBatch.get(outputStreams[i]).put(s, new Batch(batchSize, 1));
					} else {
						int bS = batchSizes.get(outputStreams[i]).intValue();
						if(bS > 0) {
							currentBatch.get(outputStreams[i]).put(s, new Batch(bS, 1));
						}
					}
				} else {
					for(Set<Integer> s : old.keySet()) {
						Set<Integer> s2 = new HashSet<Integer>();
						s2.addAll(s);
						s2.add(tid);
						if(batchSizes == null) {
							currentBatch.get(outputStreams[i]).put(s2, new Batch(batchSize, 1));
						} else {
							int bS = batchSizes.get(outputStreams[i]).intValue();
							if(bS > 0) {
								currentBatch.get(outputStreams[i]).put(s2, new Batch(bS, 1));
							}
						}
					}
				}
			}
			when(context.getComponentTasks(consumerId)).thenReturn(consumerTasks);
			taskIds.put(consumerId, consumerTasks);
			
			for(int j = 0; j < numberOfDistinctValues; ++j) {
				when(
					StormConnector.getFieldsGroupingReceiverTaskId(any(WorkerTopologyContext.class), eq(this.sourceId),
						eq(outputStreams[i]), eq(consumerId), eq(partitionsTuple[j]))).thenReturn(
					consumerTasks.get(j % numberOfConsumerTasks[i]));
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
		
		when(new Boolean(grouping.is_set_fields())).thenReturn(new Boolean(true));
		
		
		
		Map<String, List<Object>> expectedResult = new HashMap<String, List<Object>>();
		for(String s : streams.keySet()) {
			expectedResult.put(s, new LinkedList<Object>());
		}
		
		TestBatchCollector collector;
		int maxBatchSize = 0;
		if(batchSizes == null) {
			collector = new TestBatchCollector(context, batchSize);
			maxBatchSize = batchSize;
		} else {
			collector = new TestBatchCollector(context, batchSizes);
			for(Integer b : batchSizes.values()) {
				int bS = b.intValue();
				if(bS > maxBatchSize) {
					maxBatchSize = bS;
				}
			}
		}
		
		final int numberOfTuples = numberOfDistinctValues * maxBatchSize * 20
			+ this.r.nextInt(numberOfDistinctValues * maxBatchSize * 10);
		for(int i = 0; i < numberOfTuples; ++i) {
			final int index = this.r.nextInt(numberOfConsumerTasks.length);
			
			Values tuple = new Values(new Integer(this.r.nextInt(numberOfDistinctValues)));
			String outputStream = outputStreams[index];
			collector.tupleEmit(outputStream, null, tuple, null);
			
			for(Entry<String, List<String>> s : streams.entrySet()) {
				if(s.getKey().equals(outputStream)) {
					Set<Integer> batchKey = new HashSet<Integer>();
					
					for(int k = 0; k < numberOfConsumerTasks.length; ++k) {
						if(outputStreams[k].equals(outputStream)) {
							batchKey.add(taskIds.get(consumerPrefix + k).get(
								((Integer)tuple.get(0)).intValue() % numberOfConsumerTasks[k]));
						}
					}
					
					Batch batch = currentBatch.get(outputStream).get(batchKey);
					if(batch != null) {
						batch.addTuple(tuple);
						if(batch.isFull()) {
							expectedResult.get(outputStream).add(batch);
							if(batchSizes == null) {
								currentBatch.get(outputStream).put(batchKey, new Batch(batchSize, 1));
							} else {
								currentBatch.get(outputStream).put(batchKey,
									new Batch(batchSizes.get(outputStream).intValue(), 1));
							}
						}
					} else {
						expectedResult.get(outputStream).add(tuple);
					}
				}
			}
		}
		
		
		
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			Assert.assertEquals(expectedResult.get(outputStreams[i]), collector.resultBuffer.get(outputStreams[i]));
		}
	}
	
	@Test
	public void testEmitFieldGroupingViaDirectEmitSimple() {
		this.prepareAndRunTestEmitFieldsGroupingViaDirectEmitDefaultOutputStream(this.generateConsumerTasks(1, 1));
	}
	
	@Test
	public void testEmitFieldGroupingViaDirectEmitMultipleConsumerTasks() {
		this.prepareAndRunTestEmitFieldsGroupingViaDirectEmitDefaultOutputStream(this.generateConsumerTasks(1, 10));
	}
	
	@Test
	public void testEmitFieldGroupingViaDirectEmitMultipleConsumersNoDop() {
		this.prepareAndRunTestEmitFieldsGroupingViaDirectEmitDefaultOutputStream(this.generateConsumerTasks(10, 1));
	}
	
	@Test
	public void testEmitFieldGroupingViaDirectEmitFull() {
		this.prepareAndRunTestEmitFieldsGroupingViaDirectEmitDefaultOutputStream(this.generateConsumerTasks(3, 3));
	}
	
	private void prepareAndRunTestEmitFieldsGroupingViaDirectEmitDefaultOutputStream(int[] numberOfConsumerTasks) {
		this.runTestEmitFieldsGroupingViaDirectEmit(numberOfConsumerTasks,
			this.generateStreamIds(numberOfConsumerTasks.length, 0), null);
	}
	
	@Test
	public void testEmitFieldGroupingViaDirectEmitDistinctOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		this.runTestEmitFieldsGroupingViaDirectEmit(numberOfConsumerTasks,
			this.generateStreamIds(numberOfConsumerTasks.length, 1), null);
	}
	
	@Test
	public void testEmitFieldGroupingViaDirectEmitRandomOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		this.runTestEmitFieldsGroupingViaDirectEmit(numberOfConsumerTasks,
			this.generateStreamIds(numberOfConsumerTasks.length, 2), null);
	}
	
	@Test
	public void testEmitFieldGroupingViaDirectEmitRandomOutputStreamsDifferentBatchSizes() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(3, 3);
		String[] streamIds = this.generateStreamIds(numberOfConsumerTasks.length, 2);
		this.runTestEmitFieldsGroupingViaDirectEmit(numberOfConsumerTasks, streamIds,
			this.generateBatchSizes(streamIds));
	}
	
	private void runTestEmitFieldsGroupingViaDirectEmit(int[] numberOfConsumerTasks, String[] outputStreams, HashMap<String, Integer> batchSizes) {
		assert (numberOfConsumerTasks.length == outputStreams.length);
		assert (numberOfConsumerTasks.length < 100);
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			assert (numberOfConsumerTasks[i] < 100);
		}
		
		final int batchSize = 1 + this.r.nextInt(5);
		final String consumerPrefix = "consumer-";
		
		Map<Integer, Batch> currentBatch = new HashMap<Integer, Batch>();
		Map<String, List<Integer>> taskIds = new HashMap<String, List<Integer>>();
		Map<String, List<String>> streams = new HashMap<String, List<String>>();
		
		Grouping grouping = mock(Grouping.class);
		when(new Boolean(grouping.is_set_fields())).thenReturn(new Boolean(true));
		Grouping directGrouping = mock(Grouping.class);
		when(new Boolean(directGrouping.is_set_direct())).thenReturn(new Boolean(true));
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(this.sourceId);
		
		int maxNumberOfBatches = 1;
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			maxNumberOfBatches *= numberOfConsumerTasks[i];
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
			Map<String, Grouping> directConsumer = new HashMap<String, Grouping>();
			directConsumer.put(consumerId, directGrouping);
			
			final List<Integer> consumerTasks = new ArrayList<Integer>();
			for(int j = 0; j < numberOfConsumerTasks[i]; ++j) {
				Integer tid = new Integer(i * 100 + j);
				consumerTasks.add(tid);
				if(batchSizes == null) {
					currentBatch.put(tid, new Batch(batchSize, 1));
				} else {
					int bS = batchSizes.get(outputStreams[i]).intValue();
					if(bS > 0) {
						currentBatch.put(tid, new Batch(batchSizes.get(outputStreams[i]).intValue(), 1));
					}
				}
			}
			when(context.getComponentTasks(consumerId)).thenReturn(consumerTasks);
			taskIds.put(consumerId, consumerTasks);
			
			for(int j = 0; j < numberOfDistinctValues; ++j) {
				when(
					StormConnector.getFieldsGroupingReceiverTaskId(any(WorkerTopologyContext.class), eq(this.sourceId),
						eq(outputStreams[i]), eq(consumerId), eq(partitionsTuple[j]))).thenReturn(
					consumerTasks.get(j % numberOfConsumerTasks[i]));
			}
			
			Map<String, Grouping> streamMapping = targets.get(outputStreams[i]);
			if(streamMapping == null) {
				targets.put(outputStreams[i], consumer);
				targets.put(BatchingOutputFieldsDeclarer.STREAM_PREFIX + outputStreams[i], directConsumer);
			} else {
				streamMapping.putAll(consumer);
				targets.get(BatchingOutputFieldsDeclarer.STREAM_PREFIX + outputStreams[i]).putAll(directConsumer);
			}
			
			Fields schema = context.getComponentOutputFields(this.sourceId, outputStreams[i]);
			if(schema == null) {
				when(context.getComponentOutputFields(this.sourceId, outputStreams[i])).thenReturn(
					new Fields("attribute"));
				when(
					context.getComponentOutputFields(this.sourceId, BatchingOutputFieldsDeclarer.STREAM_PREFIX
						+ outputStreams[i])).thenReturn(new Fields("attribute"));
			}
			
			List<String> receivers = streams.get(outputStreams[i]);
			if(receivers == null) {
				receivers = new LinkedList<String>();
				streams.put(outputStreams[i], receivers);
			}
			receivers.add(consumerId);
		}
		when(context.getThisTargets()).thenReturn(targets);
		
		
		
		Map<Integer, List<Object>> expectedResultPerTask = new HashMap<Integer, List<Object>>();
		for(List<Integer> ids : taskIds.values()) {
			for(Integer i : ids) {
				expectedResultPerTask.put(i, new LinkedList<Object>());
			}
		}
		
		
		
		TestBatchCollector collector;
		int maxBatchSize = 0;
		if(batchSizes == null) {
			collector = new TestBatchCollector(context, batchSize);
			maxBatchSize = batchSize;
		} else {
			collector = new TestBatchCollector(context, batchSizes);
			for(Integer b : batchSizes.values()) {
				int bS = b.intValue();
				if(bS > maxBatchSize) {
					maxBatchSize = bS;
				}
			}
		}
		
		
		
		final int numberOfTuples = numberOfDistinctValues * maxBatchSize * 20
			+ this.r.nextInt(numberOfDistinctValues * maxBatchSize * 10);
		for(int i = 0; i < numberOfTuples; ++i) {
			final int index = this.r.nextInt(numberOfConsumerTasks.length);
			
			Values tuple = new Values(new Integer(this.r.nextInt(numberOfDistinctValues)));
			String outputStream = outputStreams[index];
			collector.tupleEmit(outputStream, null, tuple, null);
			
			for(Entry<String, List<String>> s : streams.entrySet()) {
				if(s.getKey().equals(outputStream)) {
					
					for(int k = 0; k < numberOfConsumerTasks.length; ++k) {
						if(outputStreams[k].equals(outputStream)) {
							Integer tid = taskIds.get(consumerPrefix + k).get(
								((Integer)tuple.get(0)).intValue() % numberOfConsumerTasks[k]);
							
							Batch batch = currentBatch.get(tid);
							if(batch != null) {
								batch.addTuple(tuple);
								if(batch.isFull()) {
									expectedResultPerTask.get(tid).add(batch);
									if(batchSizes == null) {
										currentBatch.put(tid, new Batch(batchSize, 1));
									} else {
										currentBatch.put(tid, new Batch(batchSizes.get(outputStream).intValue(), 1));
									}
								}
							} else {
								expectedResultPerTask.get(tid).add(tuple);
							}
						}
					}
				}
			}
		}
		
		
		
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			Assert.assertNull(collector.resultBuffer.get(outputStreams[i]));
			
			for(String consumer : streams.get(outputStreams[i])) {
				for(Integer tid : taskIds.get(consumer)) {
					List<Object> resultPerTask = new LinkedList<Object>();
					
					Iterator<Integer> t = collector.taskBuffer.get(
						BatchingOutputFieldsDeclarer.STREAM_PREFIX + outputStreams[i]).iterator();
					for(Object tupleOrBatch : collector.resultBuffer.get(BatchingOutputFieldsDeclarer.STREAM_PREFIX
						+ outputStreams[i])) {
						if(t.next().intValue() == tid.intValue()) {
							resultPerTask.add(tupleOrBatch);
						}
					}
					Assert.assertEquals(expectedResultPerTask.get(tid), resultPerTask);
				}
			}
		}
	}
	
	// @Test
	// public void testCustomGrouping() {
	// Assert.fail();
	// }
	
	@Test
	public void testEmitDirectSimple() {
		this.prepareAndRunTestEmitDirectDefaultOutputStream(this.generateConsumerTasks(1, 1));
	}
	
	@Test
	public void testEmitDirectMultipleConsumerTasks() {
		this.prepareAndRunTestEmitDirectDefaultOutputStream(this.generateConsumerTasks(1, 10));
	}
	
	@Test
	public void testEmitDirectMultipleConsumersNoDop() {
		this.prepareAndRunTestEmitDirectDefaultOutputStream(this.generateConsumerTasks(10, 1));
	}
	
	@Test
	public void testEmitDirectFull() {
		this.prepareAndRunTestEmitDirectDefaultOutputStream(this.generateConsumerTasks(5, 5));
	}
	
	private void prepareAndRunTestEmitDirectDefaultOutputStream(int[] numberOfConsumerTasks) {
		this.runTestEmitDirect(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 0), null);
	}
	
	@Test
	public void testEmitDirectDistinctOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		this.runTestEmitDirect(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 1), null);
	}
	
	@Test
	public void testEmitDirectRandomOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		this.runTestEmitDirect(numberOfConsumerTasks, this.generateStreamIds(numberOfConsumerTasks.length, 2), null);
	}
	
	@Test
	public void testEmitDirectRandomOutputStreamsDifferentBatchSizes() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks(5, 5);
		String[] streamIds = this.generateStreamIds(numberOfConsumerTasks.length, 2);
		this.runTestEmitDirect(numberOfConsumerTasks, streamIds, this.generateBatchSizes(streamIds));
	}
	
	private void runTestEmitDirect(int[] numberOfConsumerTasks, String[] outputStreams, HashMap<String, Integer> batchSizes) {
		assert (numberOfConsumerTasks.length == outputStreams.length);
		assert (numberOfConsumerTasks.length < 100);
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			assert (numberOfConsumerTasks[i] < 100);
		}
		
		final String consumerPrefix = "consumer-";
		final int[] numberOfAttributes = new int[numberOfConsumerTasks.length];
		
		Grouping grouping = mock(Grouping.class);
		when(new Boolean(grouping.is_set_direct())).thenReturn(new Boolean(true));
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(this.sourceId);
		
		final int[][] taskIds = new int[numberOfConsumerTasks.length][];
		
		final int batchSize = 1 + this.r.nextInt(5);
		Map<String, List<Object>> expectedResult = new HashMap<String, List<Object>>();
		Map<Integer, Batch> currentBatch = new HashMap<Integer, Batch>();
		
		Map<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			taskIds[i] = new int[numberOfConsumerTasks[i]];
			for(int j = 0; j < taskIds[i].length; ++j) {
				taskIds[i][j] = i * 100 + j;
			}
			
			final String consumerId = consumerPrefix + i;
			
			Map<String, Grouping> consumer = new HashMap<String, Grouping>();
			consumer.put(consumerId, grouping);
			
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
			
			expectedResult.put(outputStreams[i], new LinkedList<Object>());
			for(int j = 0; j < taskIds[i].length; ++j) {
				if(batchSizes == null) {
					currentBatch.put(new Integer(taskIds[i][j]), new Batch(batchSize, numberOfAttributes[i]));
				} else {
					int bS = batchSizes.get(outputStreams[i]).intValue();
					if(bS > 0) {
						currentBatch.put(new Integer(taskIds[i][j]), new Batch(bS, numberOfAttributes[i]));
					}
				}
			}
		}
		when(context.getThisTargets()).thenReturn(targets);
		
		
		
		TestBatchCollector collector;
		int maxBatchSize = 0;
		if(batchSizes == null) {
			collector = new TestBatchCollector(context, batchSize);
			maxBatchSize = batchSize;
		} else {
			collector = new TestBatchCollector(context, batchSizes);
			for(Integer b : batchSizes.values()) {
				int bS = b.intValue();
				if(bS > maxBatchSize) {
					maxBatchSize = bS;
				}
			}
		}
		
		
		
		final int numberOfTuples = maxBatchSize * 20 + this.r.nextInt(maxBatchSize * 10);
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
			
			for(int j = 0; j < numberOfConsumerTasks.length; ++j) {
				if(outputStream.equals(outputStreams[j])) {
					int tid = taskIds[j][this.r.nextInt(taskIds[j].length)];
					collector.tupleEmitDirect(tid, outputStream, null, tuple, null);
					
					Integer tidO = new Integer(tid);
					Batch b = currentBatch.get(tidO);
					if(b != null) {
						b.addTuple(tuple);
						if(b.isFull()) {
							expectedResult.get(outputStream).add(b);
							if(batchSizes == null) {
								currentBatch.put(tidO, new Batch(batchSize, numberOfAttributes[j]));
							} else {
								currentBatch.put(tidO, new Batch(batchSizes.get(outputStream).intValue(),
									numberOfAttributes[j]));
							}
						}
					} else {
						expectedResult.get(outputStream).add(tuple);
					}
				}
			}
			
		}
		
		
		for(int i = 0; i < numberOfConsumerTasks.length; ++i) {
			List<Object> result = expectedResult.get(outputStreams[i]);
			if(result.size() == 0) {
				Assert.assertNull(collector.resultBuffer.get(outputStreams[i]));
			} else {
				Assert.assertEquals(result, collector.resultBuffer.get(outputStreams[i]));
			}
			
		}
	}
	
	@Test
	public void testFlushSimple() {
		final String secondStream = "stream-2";
		final String thirdStream = "stream-3";
		final String directStream = "direct";
		
		HashMap<String, Grouping> consumer = new HashMap<String, Grouping>();
		consumer.put("receiver", mock(Grouping.class));
		
		HashMap<String, Grouping> directConsumer = new HashMap<String, Grouping>();
		Grouping direct = mock(Grouping.class);
		when(new Boolean(direct.is_set_direct())).thenReturn(new Boolean(true));
		directConsumer.put("directReceiver", direct);
		
		Map<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		targets.put(Utils.DEFAULT_STREAM_ID, consumer);
		targets.put(secondStream, consumer);
		targets.put(thirdStream, consumer);
		targets.put(directStream, directConsumer);
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisTargets()).thenReturn(targets);
		when(context.getComponentOutputFields(null, Utils.DEFAULT_STREAM_ID)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, secondStream)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, thirdStream)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, directStream)).thenReturn(new Fields("dummy"));
		when(context.getComponentTasks("directReceiver")).thenReturn(
			Arrays.asList(new Integer[] {new Integer(0), new Integer(1), new Integer(2)}));
		
		final int batchSize = 5;
		TestBatchCollector collector = new TestBatchCollector(context, batchSize);
		
		final int numberOfTuples = 42;
		for(int i = 0; i < numberOfTuples - 2; ++i) {
			collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(i)), null);
			collector.tupleEmit(secondStream, null, new Values(new Integer(i)), null);
			collector.tupleEmit(thirdStream, null, new Values(new Integer(i)), null);
			collector.tupleEmitDirect(i % 3, directStream, null, new Values(new Integer(i)), null);
		}
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(40)), null);
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(41)), null);
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(42)), null);
		
		collector.tupleEmit(secondStream, null, new Values(new Integer(40)), null);
		collector.tupleEmit(secondStream, null, new Values(new Integer(41)), null);
		
		Assert.assertEquals(8, collector.resultBuffer.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(8, collector.resultBuffer.get(secondStream).size());
		Assert.assertEquals(8, collector.resultBuffer.get(thirdStream).size());
		Assert.assertEquals(6, collector.resultBuffer.get(directStream).size());
		
		collector.flush();
		
		Assert.assertEquals(9, collector.resultBuffer.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(9, collector.resultBuffer.get(secondStream).size());
		Assert.assertEquals(8, collector.resultBuffer.get(thirdStream).size());
		Assert.assertEquals(9, collector.resultBuffer.get(directStream).size());
	}
	
	@Test
	public void testFlushAdvanced() {
		final String secondStream = "stream-2";
		final String thirdStream = "stream-3";
		final String directStream = "direct-1";
		final String directStream2 = "direct-2";
		
		HashMap<String, Grouping> consumer = new HashMap<String, Grouping>();
		consumer.put("receiver", mock(Grouping.class));
		
		HashMap<String, Grouping> directConsumer = new HashMap<String, Grouping>();
		Grouping direct = mock(Grouping.class);
		when(new Boolean(direct.is_set_direct())).thenReturn(new Boolean(true));
		directConsumer.put("directReceiver", direct);
		
		Map<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		targets.put(Utils.DEFAULT_STREAM_ID, consumer);
		targets.put(secondStream, consumer);
		targets.put(thirdStream, consumer);
		targets.put(directStream, directConsumer);
		targets.put(directStream2, directConsumer);
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisTargets()).thenReturn(targets);
		when(context.getComponentOutputFields(null, Utils.DEFAULT_STREAM_ID)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, secondStream)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, thirdStream)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, directStream)).thenReturn(new Fields("dummy"));
		when(context.getComponentOutputFields(null, directStream2)).thenReturn(new Fields("dummy"));
		when(context.getComponentTasks("directReceiver")).thenReturn(
			Arrays.asList(new Integer[] {new Integer(0), new Integer(1), new Integer(2)}));
		
		HashMap<String, Integer> batchSizes = new HashMap<String, Integer>();
		batchSizes.put(Utils.DEFAULT_STREAM_ID, new Integer(5));
		batchSizes.put(secondStream, new Integer(0));
		batchSizes.put(thirdStream, new Integer(3));
		batchSizes.put(directStream, new Integer(5));
		TestBatchCollector collector = new TestBatchCollector(context, batchSizes);
		
		final int numberOfTuples = 42;
		for(int i = 0; i < numberOfTuples - 2; ++i) {
			collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(i)), null);
			collector.tupleEmit(secondStream, null, new Values(new Integer(i)), null);
			collector.tupleEmit(thirdStream, null, new Values(new Integer(i)), null);
			collector.tupleEmitDirect(i % 3, directStream, null, new Values(new Integer(i)), null);
			collector.tupleEmitDirect(i % 3, directStream2, null, new Values(new Integer(i)), null);
		}
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(40)), null);
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(41)), null);
		collector.tupleEmit(Utils.DEFAULT_STREAM_ID, null, new Values(new Integer(42)), null);
		
		collector.tupleEmit(secondStream, null, new Values(new Integer(40)), null);
		collector.tupleEmit(secondStream, null, new Values(new Integer(41)), null);
		
		Assert.assertEquals(8, collector.resultBuffer.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(42, collector.resultBuffer.get(secondStream).size());
		Assert.assertEquals(13, collector.resultBuffer.get(thirdStream).size());
		Assert.assertEquals(6, collector.resultBuffer.get(directStream).size());
		Assert.assertEquals(40, collector.resultBuffer.get(directStream2).size());
		
		collector.flush();
		
		Assert.assertEquals(9, collector.resultBuffer.get(Utils.DEFAULT_STREAM_ID).size());
		Assert.assertEquals(42, collector.resultBuffer.get(secondStream).size());
		Assert.assertEquals(14, collector.resultBuffer.get(thirdStream).size());
		Assert.assertEquals(9, collector.resultBuffer.get(directStream).size());
		Assert.assertEquals(40, collector.resultBuffer.get(directStream2).size());
	}
	
	@Test
	public void testKryoRegistrations() {
		Config stormConfig = mock(Config.class);
		AbstractBatchCollector.registerKryoClasses(stormConfig);
		
		verify(stormConfig).registerSerialization(Batch.class);
		verify(stormConfig).registerSerialization(BatchColumn.class);
	}
	
}
