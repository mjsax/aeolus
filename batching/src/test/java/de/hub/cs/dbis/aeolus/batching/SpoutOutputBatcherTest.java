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
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.generated.Grouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.IncSpout;
import de.hub.cs.dbis.aeolus.testUtils.RandomSpout;
import de.hub.cs.dbis.aeolus.testUtils.TestSpoutOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SpoutOutputBatcher.class)
public class SpoutOutputBatcherTest {
	private IRichSpout spoutMock;
	
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		
		this.spoutMock = mock(IRichSpout.class);
	}
	
	
	
	@Test
	public void testOpen() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		
		@SuppressWarnings("rawtypes")
		Map conf = new HashMap();
		TopologyContext context = mock(TopologyContext.class);
		spout.open(conf, context, null);
		
		verify(this.spoutMock).open(same(conf), same(context), any(SpoutBatchCollector.class));
	}
	
	@Test
	public void testClose() throws Exception {
		SpoutBatchCollector collectorMock = mock(SpoutBatchCollector.class);
		PowerMockito.whenNew(SpoutBatchCollector.class).withAnyArguments().thenReturn(collectorMock);
		
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		spout.open(null, null, null);
		spout.close();
		
		verify(this.spoutMock).close();
		verify(collectorMock).flush();
	}
	
	@Test
	public void testActivate() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		spout.activate();
		verify(this.spoutMock).activate();
	}
	
	@Test
	public void testDeactivate() throws Exception {
		SpoutBatchCollector collectorMock = mock(SpoutBatchCollector.class);
		PowerMockito.whenNew(SpoutBatchCollector.class).withAnyArguments().thenReturn(collectorMock);
		
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		spout.open(null, null, null);
		
		spout.deactivate();
		verify(this.spoutMock).deactivate();
		verify(collectorMock).flush();
	}
	
	@Test
	public void testAck() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		
		Object messageId = mock(Object.class);
		spout.ack(messageId);
		
		verify(this.spoutMock).ack(messageId);
	}
	
	@Test
	public void testFail() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		
		Object messageId = mock(Object.class);
		spout.fail(messageId);
		
		verify(this.spoutMock).fail(messageId);
	}
	
	@Test
	public void testDeclareOutputFields() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		spout.declareOutputFields(declarer);
		
		verify(this.spoutMock).declareOutputFields(any(BatchingOutputFieldsDeclarer.class));
	}
	
	@Test
	public void testGetComponentConfiguration() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(this.spoutMock, 2 + this.r.nextInt(9));
		
		final Map<String, Object> conf = new HashMap<String, Object>();
		when(spout.getComponentConfiguration()).thenReturn(conf);
		
		Map<String, Object> result = spout.getComponentConfiguration();
		
		verify(this.spoutMock).getComponentConfiguration();
		Assert.assertSame(result, conf);
	}
	
	@Test(timeout = 1000)
	public void testNextTuple() {
		final int batchSize = 2 + this.r.nextInt(9);
		final String streamId = Utils.DEFAULT_STREAM_ID;
		final String sourceId = "sourceId";
		
		Grouping groupingMock = mock(Grouping.class);
		
		HashMap<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		Map<String, Grouping> receiver = new HashMap<String, Grouping>();
		receiver.put("receiverId", groupingMock);
		targets.put(streamId, receiver);
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(sourceId);
		when(context.getThisTargets()).thenReturn(targets);
		when(context.getComponentOutputFields(sourceId, streamId)).thenReturn(new Fields("dummy"));
		
		RandomSpout userSpout = new RandomSpout(1, 1000, new String[] {Utils.DEFAULT_STREAM_ID}, this.seed);
		SpoutOutputBatcher batcher = new SpoutOutputBatcher(userSpout, batchSize);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		batcher.open(null, context, new SpoutOutputCollector(collector));
		
		batcher.nextTuple();
		
		Assert.assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
	}
	
	@Test(timeout = 1000)
	public void testNextTupleNoEmit() {
		final int batchSize = 6 + this.r.nextInt(5);
		final int skipInterval = 2 + this.r.nextInt(4);
		
		final String streamId = Utils.DEFAULT_STREAM_ID;
		final String sourceId = "sourceId";
		
		
		Map<String, Grouping> receiver = new HashMap<String, Grouping>();
		receiver.put("receiverId", mock(Grouping.class));
		
		HashMap<String, Map<String, Grouping>> targets = new HashMap<String, Map<String, Grouping>>();
		targets.put(streamId, receiver);
		
		TopologyContext context = mock(TopologyContext.class);
		when(context.getThisComponentId()).thenReturn(sourceId);
		when(context.getThisTargets()).thenReturn(targets);
		when(context.getComponentOutputFields(sourceId, streamId)).thenReturn(new Fields("dummy"));
		
		IncSpout userSpout = new IncSpout(skipInterval);
		SpoutOutputBatcher batcher = new SpoutOutputBatcher(userSpout, batchSize);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		batcher.open(null, context, new SpoutOutputCollector(collector));
		
		int iterations = (int)Math.ceil(batchSize / (skipInterval - 1.));
		for(int i = 1; i < iterations; ++i) {
			batcher.nextTuple();
			Assert.assertNull(collector.output.get(Utils.DEFAULT_STREAM_ID));
		}
		batcher.nextTuple();
		
		Assert.assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
	}
	
	@Test
	public void testKryoRegistrations() {
		Config stormConfig = mock(Config.class);
		SpoutOutputBatcher.registerKryoClasses(stormConfig);
		
		verify(stormConfig).registerSerialization(Batch.class);
		verify(stormConfig).registerSerialization(BatchColumn.class);
	}
	
}
