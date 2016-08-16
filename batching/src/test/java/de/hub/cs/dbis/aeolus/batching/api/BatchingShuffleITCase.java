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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.RandomSpout;





/**
 * @author mjsax
 */
public class BatchingShuffleITCase {
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	
	
	@SuppressWarnings("rawtypes")
	@Test(timeout = 30000)
	public void testShuffle() {
		final String topologyName = "testTopology";
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
		
		builder.setBolt("Bolt", new InputDebatcher(new VerifyBolt(new Fields("a"), null)), boltDop)
			.shuffleGrouping(VerifyBolt.SPOUT_ID, "stream1").shuffleGrouping(VerifyBolt.BATCHING_SPOUT_ID, "stream2");
		
		cluster.submitTopology(topologyName, new HashMap(), builder.createTopology());
		
		Utils.sleep(10 * 1000);
		cluster.killTopology(topologyName);
		Utils.sleep(5 * 1000); // give "kill" some time to clean up; otherwise, test might hang and time out
		cluster.shutdown();
		
		Assert.assertEquals(new LinkedList<String>(), VerifyBolt.errorMessages);
		Assert.assertTrue(VerifyBolt.matchedTuples > 0);
	}
	
}
