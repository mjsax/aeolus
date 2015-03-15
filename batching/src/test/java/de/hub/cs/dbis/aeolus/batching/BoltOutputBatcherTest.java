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
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * @author Matthias J. Sax
 */
public class BoltOutputBatcherTest {
	private IRichBolt boltMock;
	private BoltOutputBatcher bolt;
	
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		
		this.boltMock = mock(IRichBolt.class);
		this.bolt = new BoltOutputBatcher(this.boltMock, 2 + this.r.nextInt(8));
	}
	
	
	
	@Test
	public void testOpen() {
		@SuppressWarnings("rawtypes")
		Map conf = new HashMap();
		TopologyContext context = mock(TopologyContext.class);
		this.bolt.prepare(conf, context, null);
		
		verify(this.boltMock).prepare(same(conf), same(context), any(BoltBatchCollector.class));
	}
	
	@Test
	public void testExecute() {
		Tuple input = mock(Tuple.class);
		this.bolt.execute(input);
		verify(this.boltMock).execute(input);
	}
	
	@Test
	public void testCleanup() {
		this.bolt.cleanup();
		verify(this.boltMock).cleanup();
	}
	
	@Test
	public void testDeclareOutputFields() {
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		this.bolt.declareOutputFields(declarer);
		verify(this.boltMock).declareOutputFields(declarer);
	}
	
	@Test
	public void testGetComponentConfiguration() {
		final Map<String, Object> conf = new HashMap<String, Object>();
		when(this.bolt.getComponentConfiguration()).thenReturn(conf);
		
		Map<String, Object> result = this.bolt.getComponentConfiguration();
		
		verify(this.boltMock).getComponentConfiguration();
		Assert.assertSame(result, conf);
	}
	
}
