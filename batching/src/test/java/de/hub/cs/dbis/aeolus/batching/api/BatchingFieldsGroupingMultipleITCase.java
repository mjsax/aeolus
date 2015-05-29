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
import de.hub.cs.dbis.aeolus.batching.BatchingOutputFieldsDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.RandomSpout;





/**
 * @author Matthias J. Sax
 */
public class BatchingFieldsGroupingMultipleITCase {
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
	public void testFieldsGroupingMultiple() {
		final String topologyName = "testTopology";
		final int maxValue = 1000;
		final int batchSize = 1 + this.r.nextInt(5);
		final int numberOfAttributes = 1 + this.r.nextInt(10);
		final int numberOfConsumers = 2 + this.r.nextInt(3);
		final Integer spoutDop = new Integer(1);
		final Integer[] boltDop = new Integer[numberOfConsumers];
		final Fields[] groupingFiels = new Fields[numberOfConsumers];
		final String[][] schema = new String[numberOfConsumers][];
		
		for(int i = 0; i < numberOfConsumers; ++i) {
			boltDop[i] = new Integer(1 + this.r.nextInt(5));
			
			LinkedList<String> attributes = new LinkedList<String>();
			for(int j = 0; j < numberOfAttributes; ++j) {
				attributes.add("" + (char)('a' + j));
			}
			
			schema[i] = new String[1 + this.r.nextInt(numberOfAttributes)];
			for(int j = 0; j < schema[i].length; ++j) {
				schema[i][j] = new String(attributes.remove(this.r.nextInt(attributes.size())));
			}
			groupingFiels[i] = new Fields(schema[i]);
		}
		
		
		
		LocalCluster cluster = new LocalCluster();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(VerifyBolt.SPOUT_ID, new RandomSpout(numberOfAttributes, maxValue, new String[] {"stream1"},
			this.seed), spoutDop);
		builder.setSpout(VerifyBolt.BATCHING_SPOUT_ID, new SpoutOutputBatcher(new RandomSpout(numberOfAttributes,
			maxValue, new String[] {"stream2"}, this.seed), batchSize), spoutDop);
		
		for(int i = 0; i < numberOfConsumers; ++i) {
			builder
				.setBolt("Bolt-" + i, new InputDebatcher(new VerifyBolt(new Fields(schema[i]), groupingFiels[i])),
					boltDop[i]).fieldsGrouping(VerifyBolt.SPOUT_ID, "stream1", groupingFiels[i])
				.fieldsGrouping(VerifyBolt.BATCHING_SPOUT_ID, "stream2", groupingFiels[i])
				.directGrouping(VerifyBolt.BATCHING_SPOUT_ID, BatchingOutputFieldsDeclarer.STREAM_PREFIX + "stream2");
			
		}
		
		cluster.submitTopology(topologyName, new HashMap(), builder.createTopology());
		
		Utils.sleep(10 * 1000);
		cluster.killTopology(topologyName);
		Utils.sleep(5 * 1000); // give "kill" some time to clean up; otherwise, test might hang and time out
		cluster.shutdown();
		
		Assert.assertEquals(new LinkedList<String>(), VerifyBolt.errorMessages);
		Assert.assertTrue(VerifyBolt.matchedTuples > 0);
	}
}
