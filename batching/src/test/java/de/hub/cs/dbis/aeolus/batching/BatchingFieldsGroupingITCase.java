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
 * @author Matthias J. Sax
 */
public class BatchingFieldsGroupingITCase {
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
	public void testFieldsGrouping() {
		final int maxValue = 1000;
		final int batchSize = 1 + this.r.nextInt(5);
		final int numberOfAttributes = 1 + this.r.nextInt(10);
		final Integer boltDop = new Integer(2 + this.r.nextInt(4));
		
		LinkedList<String> attributes = new LinkedList<String>();
		for(int i = 0; i < numberOfAttributes; ++i) {
			attributes.add("" + (char)('a' + i));
		}
		
		String[] schema = new String[1 + this.r.nextInt(numberOfAttributes)];
		for(int i = 0; i < schema.length; ++i) {
			schema[i] = new String(attributes.remove(this.r.nextInt(attributes.size())));
		}
		Fields groupingFiels = new Fields(schema);
		
		
		
		LocalCluster cluster = new LocalCluster();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(VerifyBolt.SPOUT_ID, new RandomSpout(numberOfAttributes, maxValue, new String[] {"stream1"},
			this.seed));
		builder.setSpout(VerifyBolt.BATCHING_SPOUT_ID, new SpoutOutputBatcher(new RandomSpout(numberOfAttributes,
			maxValue, new String[] {"stream2"}, this.seed), batchSize));
		
		builder.setBolt("Bolt", new InputDebatcher(new VerifyBolt(new Fields(schema), groupingFiels)), boltDop)
			.fieldsGrouping(VerifyBolt.SPOUT_ID, "stream1", groupingFiels)
			.fieldsGrouping(VerifyBolt.BATCHING_SPOUT_ID, "stream2", groupingFiels);
		
		cluster.submitTopology("test", new HashMap(), builder.createTopology());
		
		Utils.sleep(10 * 1000); // experienced test failure with 5 * 1000
		cluster.killTopology("test");
		cluster.shutdown();
		
		Assert.assertEquals(new LinkedList<String>(), VerifyBolt.errorMessages);
	}
}
