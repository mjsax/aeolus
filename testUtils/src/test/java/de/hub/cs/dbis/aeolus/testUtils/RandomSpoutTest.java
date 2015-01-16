/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class RandomSpoutTest {
	private final static long seed = System.currentTimeMillis();
	private final static Random r = new Random(seed);
	
	@BeforeClass
	public static void prepareStatic() {
		System.out.println("Test seed: " + seed);
	}
	
	@Test
	public void testDeclareOutputFieldsDefault() {
		RandomSpout spout = new RandomSpout(1000);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertTrue(declarer.schema.size() == 1);
		Assert.assertTrue(declarer.schema.get(0).size() == 1);
		Assert.assertTrue(declarer.schema.get(0).get(0).equals("a"));
		Assert.assertTrue(declarer.streamId.size() == 1);
		Assert.assertTrue(declarer.streamId.get(0).equals(Utils.DEFAULT_STREAM_ID));
		Assert.assertTrue(declarer.direct.size() == 1);
		Assert.assertTrue(declarer.direct.get(0).booleanValue() == false);
	}
	
	@Test
	public void testDeclareOutputFields() {
		int numberOfAttributes = 1 + r.nextInt(10);
		RandomSpout spout = new RandomSpout(numberOfAttributes, 1000);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertTrue(declarer.schema.size() == 1);
		Assert.assertTrue(declarer.schema.get(0).size() == numberOfAttributes);
		for(int i = 0; i < numberOfAttributes; ++i) {
			Assert.assertTrue(declarer.schema.get(0).get(i).equals("" + (char)(97 + i)));
		}
		Assert.assertTrue(declarer.streamId.size() == 1);
		Assert.assertTrue(declarer.streamId.get(0).equals(Utils.DEFAULT_STREAM_ID));
		Assert.assertTrue(declarer.direct.size() == 1);
		Assert.assertTrue(declarer.direct.get(0).booleanValue() == false);
	}
	
	@Test
	public void testDeclareOutputFieldsMultipleStreams() {
		int numberOfAttributes = 1 + r.nextInt(10);
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		RandomSpout spout = new RandomSpout(numberOfAttributes, 1000, streamIds);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertTrue(declarer.schema.size() == streamIds.length);
		Assert.assertTrue(declarer.streamId.size() == streamIds.length);
		Assert.assertTrue(declarer.direct.size() == streamIds.length);
		
		for(int i = 0; i < streamIds.length; ++i) {
			Assert.assertTrue(declarer.schema.get(i).size() == numberOfAttributes);
			for(int j = 0; j < numberOfAttributes; ++j) {
				Assert.assertTrue(declarer.schema.get(i).get(j).equals("" + (char)(97 + j)));
			}
			Assert.assertTrue(declarer.streamId.get(i).equals(streamIds[i]));
			Assert.assertTrue(declarer.direct.get(i).booleanValue() == false);
		}
	}
	
	@Test
	public void testExecuteDefault() {
		RandomSpout spout = new RandomSpout(100);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		for(int i = 0; i < 50; ++i) {
			spout.nextTuple();
			Assert.assertTrue(collector.output.get(Utils.DEFAULT_STREAM_ID).size() == i + 1); // size of result
			Assert.assertTrue(collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).size() == 1); // single attribute
			Assert.assertTrue(0 < ((Integer)collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).get(0)).intValue());
			Assert.assertTrue(100 >= ((Integer)collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).get(0)).intValue());
		}
		
	}
	
	@Test
	public void testExecute() {
		int numberOfAttributes = 1 + r.nextInt(10);
		RandomSpout spout = new RandomSpout(numberOfAttributes, 100);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		for(int i = 0; i < 50; ++i) {
			spout.nextTuple();
			Assert.assertTrue(collector.output.get(Utils.DEFAULT_STREAM_ID).size() == i + 1); // size of result
			Assert.assertTrue(collector.output.get(Utils.DEFAULT_STREAM_ID).get(i).size() == numberOfAttributes);
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
		int numberOfAttributes = 1 + r.nextInt(10);
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
