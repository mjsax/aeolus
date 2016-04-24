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

import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.batching.BatchingOutputFieldsDeclarer;





/**
 * @author Matthias J. Sax
 */
public class AeolusBuilderTest {
	private final String spout1 = "spout1";
	private final String bolt1 = "bolt1";
	
	private TopologyBuilder topologyBuilder;
	private AeolusBuilder aeolusBuilder;
	
	private final HashMap<String, Integer> noBatching = new HashMap<String, Integer>();
	
	private Random r;
	
	
	
	@Before
	public void prepare() {
		final long seed = System.currentTimeMillis();
		System.out.println("Test seed: " + seed);
		this.r = new Random(seed);
		
		this.topologyBuilder = new TopologyBuilder();
		this.aeolusBuilder = new AeolusBuilder();
	}
	
	
	
	@Test
	public void testCreateTopology() {
		this.aeolusBuilder.createTopology();
		// public StormTopology createTopology() {
	}
	
	@Test
	public void testSetBoltSimple() {
		IRichBolt userBolt = new TestBolt();
		
		this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt), this.noBatching));
		this.aeolusBuilder.setBolt(this.bolt1, userBolt);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetBoltBatchSize() {
		IRichBolt userBolt = new TestBolt();
		final int batchSize = this.r.nextInt(10);
		
		if(batchSize > 0) {
			this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt), batchSize));
		} else {
			this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt),
				this.noBatching));
		}
		this.aeolusBuilder.setBolt(this.bolt1, userBolt, batchSize);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetBoltBatchSizes() {
		IRichBolt userBolt = new TestBolt();
		HashMap<String, Integer> batchSizes = new HashMap<String, Integer>();
		
		this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt), batchSizes));
		this.aeolusBuilder.setBolt(this.bolt1, userBolt, batchSizes);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetBoltPrallelism() {
		IRichBolt userBolt = new TestBolt();
		final Integer dop = new Integer(1 + this.r.nextInt(5));
		
		this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt), this.noBatching),
			dop);
		this.aeolusBuilder.setBolt(this.bolt1, userBolt, dop);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetBoltPrallelismBatchSize() {
		IRichBolt userBolt = new TestBolt();
		final Integer dop = new Integer(1 + this.r.nextInt(5));
		final int batchSize = this.r.nextInt(10);
		
		if(batchSize > 0) {
			this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt), batchSize),
				dop);
		} else {
			this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt),
				this.noBatching), dop);
		}
		this.aeolusBuilder.setBolt(this.bolt1, userBolt, dop, batchSize);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetBoltPrallelismBatchSizes() {
		IRichBolt userBolt = new TestBolt();
		final Integer dop = new Integer(1 + this.r.nextInt(5));
		HashMap<String, Integer> batchSizes = new HashMap<String, Integer>();
		
		this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(new InputDebatcher(userBolt), batchSizes), dop);
		this.aeolusBuilder.setBolt(this.bolt1, userBolt, dop, batchSizes);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testSetBasicBoltSimple() {
		this.aeolusBuilder.setBolt(null, mock(IBasicBolt.class));
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testSetBasicBoltParallelism() {
		this.aeolusBuilder.setBolt(null, mock(IBasicBolt.class), null);
	}
	
	@Test
	public void testSetSpoutSimple() {
		IRichSpout userSpout = new TestSpout();
		
		this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, this.noBatching));
		this.aeolusBuilder.setSpout(this.spout1, userSpout);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetSpoutBatchSize() {
		IRichSpout userSpout = new TestSpout();
		final int batchSize = this.r.nextInt(10);
		
		if(batchSize > 0) {
			this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, batchSize));
		} else {
			this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, this.noBatching));
		}
		this.aeolusBuilder.setSpout(this.spout1, userSpout, batchSize);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetSpoutBatchSizes() {
		IRichSpout userSpout = new TestSpout();
		HashMap<String, Integer> batchSizes = new HashMap<String, Integer>();
		
		this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, batchSizes));
		this.aeolusBuilder.setSpout(this.spout1, userSpout, batchSizes);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetSpoutParallelism() {
		IRichSpout userSpout = new TestSpout();
		final Integer dop = new Integer(1 + this.r.nextInt(5));
		
		this.topologyBuilder.setSpout(this.spout1, userSpout, dop);
		this.aeolusBuilder.setSpout(this.spout1, userSpout, dop);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetSpoutParallelismBatchSize() {
		IRichSpout userSpout = new TestSpout();
		final Integer dop = new Integer(1 + this.r.nextInt(5));
		final int batchSize = this.r.nextInt(10);
		
		if(batchSize > 0) {
			this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, batchSize), dop);
		} else {
			this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, this.noBatching), dop);
		}
		this.aeolusBuilder.setSpout(this.spout1, userSpout, dop, batchSize);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test
	public void testSetSpoutParallelismBatchSizes() {
		IRichSpout userSpout = new TestSpout();
		final Integer dop = new Integer(1 + this.r.nextInt(5));
		HashMap<String, Integer> batchSizes = new HashMap<String, Integer>();
		
		this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, batchSizes), dop);
		this.aeolusBuilder.setSpout(this.spout1, userSpout, dop, batchSizes);
		
		Assert.assertEquals(this.topologyBuilder.createTopology(), this.aeolusBuilder.createTopology());
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testSetStateSpout() {
		this.aeolusBuilder.setStateSpout(null, null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testSetStateSpoutParallelism() {
		this.aeolusBuilder.setStateSpout(null, null, null);
	}
	
	@Test(timeout = 30000)
	public void testShuffleFromSpout() {
		IRichSpout userSpout = new TestSpout();
		final Integer spoutDop = new Integer(1 + this.r.nextInt(5));
		final int spoutBatchSize = this.r.nextInt(10);
		
		IRichBolt userBolt = new TestBolt();
		final Integer boltDop = new Integer(1 + this.r.nextInt(5));
		final int boltBatchSize = this.r.nextInt(10);
		
		
		
		this.aeolusBuilder.setSpout(this.spout1, userSpout, spoutDop, spoutBatchSize);
		this.aeolusBuilder.setBolt(this.bolt1, userBolt, boltDop, boltBatchSize).shuffleGrouping(this.spout1);
		
		
		
		userBolt = new InputDebatcher(userBolt);
		if(spoutBatchSize > 0) {
			this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, spoutBatchSize), spoutDop);
		} else {
			this.topologyBuilder.setSpout(this.spout1, new SpoutOutputBatcher(userSpout, this.noBatching), spoutDop);
		}
		BoltDeclarer declarer;
		if(boltBatchSize > 0) {
			declarer = this.topologyBuilder
				.setBolt(this.bolt1, new BoltOutputBatcher(userBolt, boltBatchSize), boltDop);
		} else {
			declarer = this.topologyBuilder.setBolt(this.bolt1, new BoltOutputBatcher(userBolt, this.noBatching),
				boltDop);
		}
		declarer.shuffleGrouping(this.spout1).directGrouping(this.spout1,
			BatchingOutputFieldsDeclarer.STREAM_PREFIX + Utils.DEFAULT_STREAM_ID);
		
		StormTopology topology = this.topologyBuilder.createTopology();
		StormTopology batchedTopology = this.aeolusBuilder.createTopology();
		Assert.assertEquals(topology, batchedTopology);
		
		// additional test to see if correct streamId are used to connect bolts (streamId are not checked in
		// .createTopology())
		LocalCluster cluster = new LocalCluster();
		final String topologyName = "topology";
		final String batchedName = "batchedTopology";
		
		cluster.submitTopology(topologyName, new Config(), topology);
		Utils.sleep(500);
		cluster.killTopology(topologyName);
		Utils.sleep(5 * 1000); // give "kill" some time to clean up; otherwise, test might hang and time out
		
		cluster.submitTopology(batchedName, new Config(), batchedTopology);
		Utils.sleep(500);
		cluster.killTopology(batchedName);
		Utils.sleep(5 * 1000); // give "kill" some time to clean up; otherwise, test might hang and time out
		
		cluster.shutdown();
	}
	
}
