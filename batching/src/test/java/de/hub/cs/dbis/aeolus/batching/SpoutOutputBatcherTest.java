package de.hub.cs.dbis.aeolus.batching;

/*
 * #%L
 * batching
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

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import de.hub.cs.dbis.aeolus.testUtils.RandomSpout;





/**
 * @author Matthias J. Sax
 */
public class SpoutOutputBatcherTest {
	private static IRichSpout spoutMockStatic;
	
	private long seed;
	private Random r;
	
	@BeforeClass
	public static void prepareStatic() {
		spoutMockStatic = mock(IRichSpout.class);
	}
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
		
	}
	
	@Test
	public void testOpen() {
		SpoutOutputBatcher spout = new SpoutOutputBatcher(spoutMockStatic, 10);
		
		// spout.open(null, null, null);
	}
	
	@Test
	public void testClose() {
		
	}
	
	@Test
	public void testActivate() {
		
	}
	
	@Test
	public void testDeactivate() {
		
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testNextTupleSimpleShuffle() {
		final int maxValue = 1000;
		final int batchSize = 1 + this.r.nextInt(5);
		final int numberOfAttributes = 1;
		final Integer spoutDop = new Integer(1);
		final Integer boltDop = new Integer(1);
		
		LocalCluster cluster = new LocalCluster();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(VerifyBolt.SPOUT_ID, new RandomSpout(numberOfAttributes, maxValue, new String[] {"stream1"},
			this.seed), spoutDop);
		builder.setSpout(VerifyBolt.BATCHING_SPOUT_ID, new SpoutOutputBatcher(new RandomSpout(numberOfAttributes,
			maxValue, new String[] {"stream2"}, this.seed), batchSize), spoutDop);
		
		builder.setBolt("Bolt", new InputDebatcher(new VerifyBolt(new Fields("a"))), boltDop)
			.shuffleGrouping(VerifyBolt.SPOUT_ID, "stream1").shuffleGrouping(VerifyBolt.BATCHING_SPOUT_ID, "stream2");
		
		cluster.submitTopology("test", new HashMap(), builder.createTopology());
		
		try {
			Thread.sleep(2 * 1000);
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
		
		// TODO: add batch flush test
		cluster.killTopology("test");
		cluster.shutdown();
	}
	
	@Test
	public void testAck() {
		
	}
	
	@Test
	public void testFail() {
		
	}
	
	@Test
	public void testDeclareOutputFields() {
		
	}
	
	@Test
	public void testGetComponentConfiguration() {}
	
}
