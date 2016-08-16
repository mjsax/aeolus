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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
public class RandomSpoutTest {
	private long seed;
	private Random r;
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	@Test
	public void testDeclareOutputFieldsDefault() {
		RandomSpout spout = new RandomSpout(1000);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(1, declarer.schemaBuffer.size());
		Assert.assertEquals(1, declarer.schemaBuffer.get(0).size());
		Assert.assertEquals("a", declarer.schemaBuffer.get(0).get(0));
		Assert.assertEquals(1, declarer.streamIdBuffer.size());
		Assert.assertEquals(Utils.DEFAULT_STREAM_ID, declarer.streamIdBuffer.get(0));
		Assert.assertEquals(1, declarer.directBuffer.size());
		Assert.assertFalse(declarer.directBuffer.get(0).booleanValue());
	}
	
	@Test
	public void testDeclareOutputFields() {
		int numberOfAttributes = 1 + this.r.nextInt(10);
		RandomSpout spout = new RandomSpout(numberOfAttributes, 1000);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(1, declarer.schemaBuffer.size());
		Assert.assertEquals(numberOfAttributes, declarer.schemaBuffer.get(0).size());
		for(int i = 0; i < numberOfAttributes; ++i) {
			Assert.assertEquals("" + (char)(97 + i), declarer.schemaBuffer.get(0).get(i));
		}
		Assert.assertEquals(1, declarer.streamIdBuffer.size());
		Assert.assertEquals(Utils.DEFAULT_STREAM_ID, declarer.streamIdBuffer.get(0));
		Assert.assertEquals(1, declarer.directBuffer.size());
		Assert.assertFalse(declarer.directBuffer.get(0).booleanValue());
	}
	
	@Test
	public void testDeclareOutputFieldsMultipleStreams() {
		int numberOfAttributes = 1 + this.r.nextInt(10);
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		RandomSpout spout = new RandomSpout(numberOfAttributes, 1000, streamIds);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(streamIds.length, declarer.schemaBuffer.size());
		Assert.assertEquals(streamIds.length, declarer.streamIdBuffer.size());
		Assert.assertEquals(streamIds.length, declarer.directBuffer.size());
		
		for(int i = 0; i < streamIds.length; ++i) {
			Assert.assertEquals(numberOfAttributes, declarer.schemaBuffer.get(i).size());
			for(int j = 0; j < numberOfAttributes; ++j) {
				Assert.assertEquals("" + (char)(97 + j), declarer.schemaBuffer.get(i).get(j));
			}
			Assert.assertEquals(streamIds[i], declarer.streamIdBuffer.get(i));
			Assert.assertFalse(declarer.directBuffer.get(i).booleanValue());
		}
	}
	
	@Test
	public void testExecuteDefault() {
		RandomSpout spout = new RandomSpout(100);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		for(int i = 0; i < 50; ++i) {
			spout.nextTuple();
			Assert.assertEquals(i + 1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
			Assert.assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).size());
			Assert.assertTrue(0 < ((Integer)collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).get(0)).intValue());
			Assert.assertTrue(100 >= ((Integer)collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).get(0)).intValue());
		}
		
	}
	
	@Test
	public void testExecute() {
		int numberOfAttributes = 1 + this.r.nextInt(10);
		RandomSpout spout = new RandomSpout(numberOfAttributes, 100);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		for(int i = 0; i < 50; ++i) {
			spout.nextTuple();
			Assert.assertEquals(i + 1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
			Assert.assertEquals(numberOfAttributes, collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).size());
			for(int j = 0; j < numberOfAttributes; ++j) {
				Assert
					.assertTrue(0 < ((Integer)collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).get(j)).intValue());
				Assert.assertTrue(100 >= ((Integer)collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).get(j))
					.intValue());
			}
		}
		
	}
	
	@Test
	public void testExecuteMultipleStreams() {
		int numberOfAttributes = 1 + this.r.nextInt(10);
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		RandomSpout spout = new RandomSpout(numberOfAttributes, 100, streamIds);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		for(int i = 0; i < 50; ++i) {
			spout.nextTuple();
			for(String stream : streamIds) {
				Assert.assertTrue(collector.output.get(stream).size() == i + 1); // size of result
				Assert.assertTrue(collector.output.get(stream).get(i).size() == numberOfAttributes);
				for(int j = 0; j < numberOfAttributes; ++j) {
					Assert.assertTrue(0 < ((Integer)collector.output.get(stream).get(i).get(j)).intValue());
					Assert.assertTrue(100 >= ((Integer)collector.output.get(stream).get(i).get(j)).intValue());
				}
			}
		}
		
	}
	
}
