package de.hub.cs.dbis.aeolus.queries.utils;

/*
 * #%L
 * utils
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.monitoring.TimestampOrderChecker;
import de.hub.cs.dbis.aeolus.testUtils.ForwardBolt;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class TimestampMergerTest {
	private static final Map<String, Object> boltConfig = new HashMap<String, Object>();
	
	private static TimestampOrderChecker checker;
	private static IRichBolt boltMockStatic;
	private static ForwardBolt forwarder = new ForwardBolt(new Fields("ts"));
	
	private static int tsIndex;
	private static boolean duplicates;
	
	private final static String bolt = "b";
	
	private final static List<List<Object>> result = new LinkedList<List<Object>>();
	private static LinkedList<Tuple>[] input;
	
	private long seed;
	private Random r;
	
	@Mock private GeneralTopologyContext contextMock;
	@Mock private TopologyContext topologyContextMock;
	
	
	
	@BeforeClass
	public static void prepareStatic() {
		long seed = System.currentTimeMillis();
		Random r = new Random(seed);
		System.out.println("Static test seed: " + seed);
		
		tsIndex = r.nextInt();
		if(tsIndex < 0) {
			tsIndex *= -1;
		}
		
		duplicates = r.nextBoolean();
		
		boltMockStatic = mock(IRichBolt.class);
		when(boltMockStatic.getComponentConfiguration()).thenReturn(boltConfig);
		
		checker = new TimestampOrderChecker(boltMockStatic, tsIndex, duplicates);
	}
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		forwarder.prepare(TimestampMergerTest.boltConfig, null, null);
	}
	
	
	
	private int mockInputs(int numberOfProducers, int numberOfTasks, boolean tsIndexOrName, int minNumberOfAttributes, int maxNumberOfAttributes) {
		assert numberOfProducers > 0;
		assert numberOfTasks != 0;
		
		int createdTasks = 0;
		
		Map<GlobalStreamId, Grouping> mapping = new HashMap<GlobalStreamId, Grouping>();
		
		for(int i = 0; i < numberOfProducers; ++i) {
			String b = bolt + i;
			mapping.put(new GlobalStreamId(b, null), null);
			
			int n = numberOfTasks;
			if(n < 0) {
				n = 1 + this.r.nextInt(-numberOfTasks);
			}
			
			List<Integer> taskList = new LinkedList<Integer>();
			for(int j = createdTasks; j < createdTasks + n; ++j) {
				taskList.add(new Integer(j));
			}
			createdTasks += n;
			
			when(this.contextMock.getComponentId(anyInt())).thenReturn(b);
			when(this.topologyContextMock.getComponentTasks(b)).thenReturn(taskList);
		}
		
		if(tsIndexOrName) {
			when(this.contextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("ts"));
		} else {
			for(int i = 0; i < numberOfProducers; ++i) {
				int numberOfAttributes = minNumberOfAttributes
					+ this.r.nextInt(maxNumberOfAttributes - minNumberOfAttributes + 1);
				List<String> schema = new ArrayList<String>(numberOfAttributes);
				for(int j = 0; j < numberOfAttributes; ++j) {
					schema.add("a" + j);
				}
				schema.set(this.r.nextInt(numberOfAttributes), "ts");
				
				when(this.contextMock.getComponentOutputFields(eq(bolt + i), anyString())).thenReturn(
					new Fields(schema));
			}
		}
		when(this.topologyContextMock.getThisSources()).thenReturn(mapping);
		
		return createdTasks;
	}
	
	@Test
	public void testExecuteMergeStrictSingleTaskSimple() {
		this.testExecuteMerge(1, 1, 0.0, this.r.nextBoolean(), 1, 1);
	}
	
	@Test
	public void testExecuteMergeStrictSingleTask() {
		this.testExecuteMerge(1, 1, 0.0, this.r.nextBoolean(), 3, 10);
	}
	
	@Test
	public void testExecuteMergeStrictSingleProducerSimple() {
		this.testExecuteMerge(1, 2, 0.0, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(1, 3 + this.r.nextInt(7), 0.0, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(1, -(3 + this.r.nextInt(7)), 0.0, this.r.nextBoolean(), 1, 1);
	}
	
	@Test
	public void testExecuteMergeStrictSingleProducer() {
		this.testExecuteMerge(1, 2, 0.0, this.r.nextBoolean(), 3, 10);
		this.testExecuteMerge(1, 3 + this.r.nextInt(7), 0.0, this.r.nextBoolean(), 3, 10);
		this.testExecuteMerge(1, -(3 + this.r.nextInt(7)), 0.0, this.r.nextBoolean(), 3, 10);
	}
	
	@Test
	public void testExecuteMergeStrictMultipleProducersSimple() {
		this.testExecuteMerge(2, 1, 0.0, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(3 + this.r.nextInt(7), 1, 0.0, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(3 + this.r.nextInt(7), -(3 + this.r.nextInt(7)), 0.0, this.r.nextBoolean(), 1, 1);
	}
	
	@Test
	public void testExecuteMergeStrictMultipleProducers() {
		this.testExecuteMerge(2, 1, 0.0, this.r.nextBoolean(), 3, 10);
		this.testExecuteMerge(3 + this.r.nextInt(7), 1, 0.0, this.r.nextBoolean(), 3, 10);
		this.testExecuteMerge(3 + this.r.nextInt(7), -(3 + this.r.nextInt(7)), 0.0, this.r.nextBoolean(), 3, 10);
	}
	
	@Test
	public void testExecuteMergeSingleTaskSimple() {
		this.testExecuteMerge(1, 1, 0.3, this.r.nextBoolean(), 1, 1);
	}
	
	@Test
	public void testExecuteMergeSingleTask() {
		this.testExecuteMerge(1, 1, 0.3, this.r.nextBoolean(), 3, 10);
	}
	
	@Test
	public void testExecuteMergeSingleProducerSimple() {
		this.testExecuteMerge(1, 2, 0.3, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(1, 3 + this.r.nextInt(7), 0.3, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(1, -(3 + this.r.nextInt(7)), 0.3, this.r.nextBoolean(), 1, 1);
	}
	
	@Test
	public void testExecuteMergeMultipleProducersSimple() {
		this.testExecuteMerge(2, 1, 0.3, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(3 + this.r.nextInt(7), 1, 0.3, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(3 + this.r.nextInt(7), -(3 + this.r.nextInt(7)), 0.3, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(3 + this.r.nextInt(7), -(3 + this.r.nextInt(7)), 1, this.r.nextBoolean(), 1, 1);
		this.testExecuteMerge(3 + this.r.nextInt(7), -(3 + this.r.nextInt(7)), this.r.nextDouble(),
			this.r.nextBoolean(), 1, 1);
	}
	
	@SuppressWarnings("unchecked")
	private void testExecuteMerge(int numberOfProducers, int numberOfTasks, double duplicatesFraction, boolean tsIndexOrName, int minNumberOfAttributes, int maxNumberOfAttributes) {
		int createdTasks = this.mockInputs(numberOfProducers, numberOfTasks, tsIndexOrName, minNumberOfAttributes,
			maxNumberOfAttributes);
		
		final int numberOfTuples = createdTasks * 10 + this.r.nextInt(createdTasks * (1 + this.r.nextInt(10)));
		TimestampOrderChecker checkerBolt;
		TimestampMerger merger;
		if(tsIndexOrName) {
			checkerBolt = new TimestampOrderChecker(forwarder, 0, duplicatesFraction != 0);
			merger = new TimestampMerger(checkerBolt, 0);
		} else {
			checkerBolt = new TimestampOrderChecker(forwarder, "ts", duplicatesFraction != 0);
			merger = new TimestampMerger(checkerBolt, "ts");
		}
		TestOutputCollector collector = new TestOutputCollector();
		
		merger.prepare(null, this.topologyContextMock, new OutputCollector(collector));
		
		
		input = new LinkedList[createdTasks];
		for(int i = 0; i < createdTasks; ++i) {
			input[i] = new LinkedList<Tuple>();
		}
		result.clear();
		
		
		int numberDistinctValues = 1;
		int counter = 0;
		while(true) {
			int taskId = this.r.nextInt(createdTasks);
			
			Fields schema = this.contextMock.getComponentOutputFields(this.contextMock.getComponentId(taskId), null);
			int numberOfAttributes = schema.size();
			List<Object> value = new ArrayList<Object>(numberOfAttributes);
			for(int i = 0; i < numberOfAttributes; ++i) {
				value.add(new Character((char)(32 + this.r.nextInt(95))));
			}
			Long ts = new Long(numberDistinctValues - 1);
			value.set(schema.fieldIndex("ts"), ts);
			
			result.add(value);
			input[taskId].add(new TupleImpl(this.contextMock, value, taskId, null));
			
			if(++counter == numberOfTuples) {
				break;
			}
			
			if(1 - this.r.nextDouble() > duplicatesFraction) {
				++numberDistinctValues;
			}
		}
		
		
		
		int[] max = new int[createdTasks];
		int[][] bucketSums = new int[createdTasks][numberDistinctValues];
		for(int i = 0; i < numberOfTuples; ++i) {
			int taskId = this.r.nextInt(createdTasks);
			
			while(input[taskId].size() == 0) {
				taskId = (taskId + 1) % createdTasks;
			}
			
			Tuple t = input[taskId].removeFirst();
			max[taskId] = t.getLongByField("ts").intValue();
			++bucketSums[taskId][max[taskId]];
			merger.execute(t);
		}
		
		
		
		int stillBuffered = numberOfTuples;
		int smallestMax = Collections.min(Arrays.asList(ArrayUtils.toObject(max))).intValue();
		for(int i = 0; i < createdTasks; ++i) {
			for(int j = 0; j <= smallestMax; ++j) {
				stillBuffered -= bucketSums[i][j];
			}
		}
		
		Assert.assertEquals(result.subList(0, result.size() - stillBuffered),
			collector.output.get(Utils.DEFAULT_STREAM_ID));
		Assert.assertTrue(collector.acked.size() == numberOfTuples - stillBuffered);
		Assert.assertTrue(collector.failed.size() == 0);
		
	}
	
	@Test
	public void testCleanup() {
		checker.cleanup();
		verify(boltMockStatic).cleanup();
		verify(boltMockStatic, atMost(1)).cleanup();
	}
	
	@Test
	public void testDeclareOutputFields() {
		OutputFieldsDeclarer declarerMock = mock(OutputFieldsDeclarer.class);
		checker.declareOutputFields(declarerMock);
		verify(boltMockStatic).declareOutputFields(declarerMock);
		verify(boltMockStatic, atMost(1)).declareOutputFields(any(OutputFieldsDeclarer.class));
	}
	
	@Test
	public void testGetComponentConfiguration() {
		Assert.assertSame(boltConfig, checker.getComponentConfiguration());
	}
	
	@Test
	public void testPrepare() {
		Map<?, ?> config = new HashMap<Object, Object>();
		TopologyContext context = mock(TopologyContext.class);
		OutputCollector collector = mock(OutputCollector.class);
		checker.prepare(config, context, collector);
		verify(boltMockStatic).prepare(config, context, collector);
		verify(boltMockStatic, atMost(1)).prepare(config, context, collector);
	}
	
}
