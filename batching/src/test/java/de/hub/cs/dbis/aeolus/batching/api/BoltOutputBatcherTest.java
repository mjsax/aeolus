/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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
package de.hub.cs.dbis.aeolus.batching.api;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.aeolus.batching.Batch;
import de.hub.cs.dbis.aeolus.batching.BatchColumn;
import de.hub.cs.dbis.aeolus.batching.BatchOutputCollector;
import de.hub.cs.dbis.aeolus.batching.BatchingOutputFieldsDeclarer;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BoltOutputBatcher.class)
public class BoltOutputBatcherTest {
	private IRichBolt boltMock;
	private final HashMap<String, Integer> noBatching = new HashMap<String, Integer>();
	
	
	
	@Before
	public void prepare() {
		this.boltMock = mock(IRichBolt.class);
	}
	
	
	
	@Test
	public void testPrepare() {
		BoltOutputBatcher bolt = new BoltOutputBatcher(this.boltMock, this.noBatching);
		
		@SuppressWarnings("rawtypes")
		Map conf = new HashMap();
		TopologyContext context = mock(TopologyContext.class);
		bolt.prepare(conf, context, null);
		
		verify(this.boltMock).prepare(same(conf), same(context), any(BatchOutputCollector.class));
	}
	
	@Test
	public void testExecute() {
		BoltOutputBatcher bolt = new BoltOutputBatcher(this.boltMock, this.noBatching);
		
		Tuple input = mock(Tuple.class);
		bolt.execute(input);
		
		verify(this.boltMock).execute(input);
	}
	
	@Test
	public void testCleanup() throws Exception {
		BoltOutputBatcher bolt = new BoltOutputBatcher(this.boltMock, this.noBatching);
		
		BatchOutputCollector collectorMock = mock(BatchOutputCollector.class);
		PowerMockito.whenNew(BatchOutputCollector.class).withAnyArguments().thenReturn(collectorMock);
		
		bolt.prepare(null, null, null);
		bolt.cleanup();
		
		verify(this.boltMock).cleanup();
		verify(collectorMock).flush();
	}
	
	@Test
	public void testDeclareOutputFields() {
		BoltOutputBatcher bolt = new BoltOutputBatcher(this.boltMock, this.noBatching);
		
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		bolt.declareOutputFields(declarer);
		
		verify(this.boltMock).declareOutputFields(any(BatchingOutputFieldsDeclarer.class));
	}
	
	@Test
	public void testGetComponentConfiguration() {
		BoltOutputBatcher bolt = new BoltOutputBatcher(this.boltMock, this.noBatching);
		
		final Map<String, Object> conf = new HashMap<String, Object>();
		when(bolt.getComponentConfiguration()).thenReturn(conf);
		
		Map<String, Object> result = bolt.getComponentConfiguration();
		
		verify(this.boltMock).getComponentConfiguration();
		Assert.assertSame(result, conf);
	}
	
	@Test
	public void testKryoRegistrations() {
		Config stormConfig = mock(Config.class);
		BoltOutputBatcher.registerKryoClasses(stormConfig);
		
		verify(stormConfig).registerSerialization(Batch.class);
		verify(stormConfig).registerSerialization(BatchColumn.class);
	}
	
}
