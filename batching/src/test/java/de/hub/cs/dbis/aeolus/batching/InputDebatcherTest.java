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
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.ForwardBolt;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;





/**
 * @author Matthias J. Sax
 */
public class InputDebatcherTest {
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		
	}
	
	
	
	@Test
	public void testOpen() {
		IRichBolt boltMock = mock(IRichBolt.class);
		InputDebatcher bolt = new InputDebatcher(boltMock);
		
		@SuppressWarnings("rawtypes")
		Map conf = new HashMap();
		TopologyContext context = mock(TopologyContext.class);
		bolt.prepare(conf, context, null);
		
		verify(boltMock).prepare(same(conf), same(context), any(BoltBatchCollector.class));
	}
	
	@Test
	public void testExecuteNoBatching() {
		InputDebatcher bolt = new InputDebatcher(new ForwardBolt(new Fields("dummy")));
		
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		List<Values> expectedResult = new LinkedList<Values>();
		
		final int numberOfTuples = 10;
		for(int i = 0; i < numberOfTuples; ++i) {
			Values value = new Values(new Integer(this.r.nextInt()));
			expectedResult.add(value);
			
			Tuple input = mock(Tuple.class);
			when(input.getValues()).thenReturn(value);
			
			bolt.execute(input);
		}
		
		Assert.assertEquals(expectedResult, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testExecuteSimpleBatching() {
		final int batchSize = 2 + this.r.nextInt(8);
		final int numberOfAttributes = 2 + this.r.nextInt(3);
		final int numberOfTuples = batchSize * 10;
		
		String[] inputSchema = new String[numberOfAttributes];
		for(int i = 0; i < inputSchema.length; ++i) {
			inputSchema[i] = "attribute-" + i;
		}
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getComponentOutputFields(any(String.class), any(String.class)))
			.thenReturn(new Fields(inputSchema));
		
		InputDebatcher bolt = new InputDebatcher(new ForwardBolt(new Fields("dummy")));
		
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, context, new OutputCollector(collector));
		
		List<Values> expectedResult = new LinkedList<Values>();
		Batch inputBatch = new Batch(batchSize, numberOfAttributes);
		
		for(int i = 0; i < numberOfTuples; ++i) {
			Values value = new Values();
			for(int j = 0; j < numberOfAttributes; ++j) {
				value.add(new Integer(this.r.nextInt()));
			}
			expectedResult.add(value);
			
			inputBatch.addTuple(value);
			if(inputBatch.isFull()) {
				Tuple input = mock(Tuple.class);
				when(input.getValues()).thenReturn((List)inputBatch);
				when(new Integer(input.size())).thenReturn(new Integer(numberOfAttributes));
				for(int j = 0; j < numberOfAttributes; ++j) {
					when(input.getValue(j)).thenReturn(inputBatch.get(j));
				}
				bolt.execute(input);
				
				inputBatch = new Batch(batchSize, numberOfAttributes);
			}
		}
		
		Assert.assertEquals(expectedResult, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testCleanup() {
		IRichBolt boltMock = mock(IRichBolt.class);
		InputDebatcher bolt = new InputDebatcher(boltMock);
		
		bolt.cleanup();
		
		verify(boltMock).cleanup();
	}
	
	@Test
	public void testDeclareOutputFields() {
		IRichBolt boltMock = mock(IRichBolt.class);
		InputDebatcher bolt = new InputDebatcher(boltMock);
		
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		bolt.declareOutputFields(declarer);
		
		verify(boltMock).declareOutputFields(declarer);
	}
	
	@Test
	public void testGetComponentConfiguration() {
		IRichBolt boltMock = mock(IRichBolt.class);
		InputDebatcher bolt = new InputDebatcher(boltMock);
		
		final Map<String, Object> conf = new HashMap<String, Object>();
		when(bolt.getComponentConfiguration()).thenReturn(conf);
		
		Map<String, Object> result = bolt.getComponentConfiguration();
		
		verify(boltMock).getComponentConfiguration();
		Assert.assertSame(result, conf);
	}
	
}
