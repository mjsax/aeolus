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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
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
	
	
	
	@Before
	public void prepareTest() {
		final long seed = System.currentTimeMillis();
		this.r = new Random(seed);
		System.out.println("Test seed: " + seed);
		
		PowerMockito.mockStatic(StormConnector.class);
	}
	
	
	
	@Test
	public void testEmitShuffleSimple() {
		this.runTestEmitShuffleDefaultOutputStream(new int[] {1});
	}
	
	@Test
	public void testEmitShuffleMultipleConsumerTasks() {
		this.runTestEmitShuffleDefaultOutputStream(new int[] {1 + this.r.nextInt(10)});
	}
	
	@Test
	public void testEmitShuffleMultipleConsumersNoDop() {
		final int numberOfConsumers = 1 + this.r.nextInt(10);
		final int[] numberOfConsumerTasks = new int[numberOfConsumers];
		for(int i = 0; i < numberOfConsumers; ++i) {
			numberOfConsumerTasks[i] = 1;
		}
		this.runTestEmitShuffleDefaultOutputStream(numberOfConsumerTasks);
		
	}
	
	@Test
	public void testEmitShuffleFull() {
		this.runTestEmitShuffleDefaultOutputStream(this.generateConsumerTasks());
	}
	
	@Test
	public void testEmitShuffleDistinctOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks();
		String[] outputStreams = new String[numberOfConsumerTasks.length];
		for(int i = 0; i < outputStreams.length; ++i) {
			outputStreams[i] = "streamId-" + i;
		}
		this.runTestEmitShuffle(numberOfConsumerTasks, outputStreams);
	}
	
	@Test
	public void testEmitShuffleRandomOutputStreams() {
		int[] numberOfConsumerTasks = this.generateConsumerTasks();
		String[] outputStreams = new String[numberOfConsumerTasks.length];
		for(int i = 0; i < outputStreams.length; ++i) {
			outputStreams[i] = "streamId-" + this.r.nextInt(outputStreams.length);
		}
		this.runTestEmitShuffle(numberOfConsumerTasks, outputStreams);
	}
	
	private int[] generateConsumerTasks() {
		final int numberOfConsumers = 1 + this.r.nextInt(5);
		final int[] numberOfConsumerTasks = new int[numberOfConsumers];
		for(int i = 0; i < numberOfConsumers; ++i) {
			numberOfConsumerTasks[i] = 1 + this.r.nextInt(5);
		}
		
		return numberOfConsumerTasks;
	}
	
	private void runTestEmitShuffleDefaultOutputStream(int[] numberOfConsumerTasks) {
		String[] outputStreams = new String[numberOfConsumerTasks.length];
		for(int i = 0; i < outputStreams.length; ++i) {
			outputStreams[i] = Utils.DEFAULT_STREAM_ID;
		}
		this.runTestEmitShuffle(numberOfConsumerTasks, outputStreams);
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
			
			// when(
			// StormConnector.getFieldsGroupingReceiverTaskId(any(WorkerTopologyContext.class), eq(this.sourceId),
			// eq(outputStreams[i]), eq(consumerId), any(List.class))).thenReturn(new Integer(0));
			
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
		
		final int numberOfTuples = 20 + this.r.nextInt(80);
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
		// Assert.fail();
	}
	
	@Test
	public void testEmitShuffeFieldGroupingCombined() {
		// Assert.fail();
	}
	
}
