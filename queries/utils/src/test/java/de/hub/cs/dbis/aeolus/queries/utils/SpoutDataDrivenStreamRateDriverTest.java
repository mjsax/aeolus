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
package de.hub.cs.dbis.aeolus.queries.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.queries.utils.DataDrivenStreamRateDriverSpout.TimeUnit;
import de.hub.cs.dbis.aeolus.testUtils.IncSpout;
import de.hub.cs.dbis.aeolus.testUtils.TestSpoutOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class SpoutDataDrivenStreamRateDriverTest {
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	
	
	@Test
	public void testForwardCalls() {
		IRichSpout worker = mock(IRichSpout.class);
		@SuppressWarnings("rawtypes")
		DataDrivenStreamRateDriverSpout driver = new DataDrivenStreamRateDriverSpout(worker, 0, TimeUnit.SECONDS);
		
		Config cfg = mock(Config.class);
		TopologyContext c = mock(TopologyContext.class);
		SpoutOutputCollector col = mock(SpoutOutputCollector.class);
		
		driver.open(cfg, c, col);
		verify(worker).open(Matchers.eq(cfg), Matchers.eq(c), Matchers.isA(DataDrivenStreamRateDriverCollector.class));
		
		driver.close();
		verify(worker).close();
		
		driver.activate();
		verify(worker).activate();
		
		driver.deactivate();
		verify(worker).deactivate();
		
		driver.nextTuple();
		verify(worker).nextTuple();
		
		Object messageId = new Object();
		driver.ack(messageId);
		verify(worker).ack(messageId);
		
		driver.fail(messageId);
		verify(worker).fail(messageId);
		
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		driver.declareOutputFields(declarer);
		verify(worker).declareOutputFields(declarer);
		
		Map<String, Object> config = worker.getComponentConfiguration();
		Assert.assertEquals(config, driver.getComponentConfiguration());
	}
	
	@Test
	public void testNextTupleFixedSecond() {
		IRichSpout worker = new IncSpout();
		DataDrivenStreamRateDriverSpout<Integer> driver = new DataDrivenStreamRateDriverSpout<Integer>(worker, 0,
			TimeUnit.SECONDS);
		
		Config cfg = mock(Config.class);
		TopologyContext c = mock(TopologyContext.class);
		SpoutOutputCollector col = mock(SpoutOutputCollector.class);
		
		driver.open(cfg, c, col);
		
		long start = System.nanoTime();
		driver.activate();
		for(int i = 0; i < 5; ++i) {
			driver.nextTuple();
		}
		long stop = System.nanoTime();
		
		Assert.assertEquals(5, (stop - start) / 1000 / 1000 / 1000, 1);
	}
	
	@SuppressWarnings("null")
	@Test
	public void testNextTuple() {
		TimeUnit units = null;
		switch(this.r.nextInt(4)) {
		case 0:
			units = TimeUnit.SECONDS;
			break;
		case 1:
			units = TimeUnit.MICROSECONDS;
			break;
		case 2:
			units = TimeUnit.MILLISECONDS;
			break;
		case 3:
			units = TimeUnit.NANOSECONDS;
			break;
		}
		
		double prob = this.r.nextDouble();
		int stepSize = 1;
		int numberOfTuples = (int)(1000000000 / units.factor() / (1 - prob));
		
		final int maxNumberOfTuplesPerSecond = 100000;
		if(numberOfTuples > maxNumberOfTuplesPerSecond) {
			stepSize = numberOfTuples / maxNumberOfTuplesPerSecond;
			numberOfTuples /= stepSize;
		}
		numberOfTuples *= 5; // run for 5 seconds
		
		
		
		IRichSpout worker = new IncSpout(prob, stepSize);
		DataDrivenStreamRateDriverSpout<Integer> driver = new DataDrivenStreamRateDriverSpout<Integer>(worker, 0, units);
		
		Config cfg = mock(Config.class);
		TopologyContext c = mock(TopologyContext.class);
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		SpoutOutputCollector col = new SpoutOutputCollector(collector);
		
		driver.open(cfg, c, col);
		
		
		driver.activate();
		long start = System.nanoTime();
		for(int i = 0; i < numberOfTuples; ++i) {
			driver.nextTuple();
		}
		long stop = System.nanoTime();
		
		long lastTS = ((Long)collector.output.get(Utils.DEFAULT_STREAM_ID).getLast().get(0)).longValue();
		Assert.assertEquals(lastTS / (1000000000 / units.factor()), (stop - start) / units.factor()
			/ (1000000000 / units.factor()), 1);
	}
	
}
