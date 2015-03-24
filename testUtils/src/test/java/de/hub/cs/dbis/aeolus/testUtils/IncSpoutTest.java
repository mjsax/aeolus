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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class IncSpoutTest {
	private long seed;
	private Random r;
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	@Test
	public void testDeclareOutputFields() {
		IncSpout spout = new IncSpout();
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(1, declarer.schemaBuffer.size());
		Assert.assertEquals(1, declarer.schemaBuffer.get(0).size());
		Assert.assertEquals("id", declarer.schemaBuffer.get(0).get(0));
		Assert.assertEquals(1, declarer.streamIdBuffer.size());
		Assert.assertEquals(Utils.DEFAULT_STREAM_ID, declarer.streamIdBuffer.get(0));
		Assert.assertEquals(1, declarer.directBuffer.size());
		Assert.assertFalse(declarer.directBuffer.get(0).booleanValue());
	}
	
	@Test
	public void testDeclareOutputFieldsMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(streamIds.length, declarer.schemaBuffer.size());
		Assert.assertEquals(streamIds.length, declarer.streamIdBuffer.size());
		Assert.assertEquals(streamIds.length, declarer.directBuffer.size());
		
		for(int i = 0; i < streamIds.length; ++i) {
			Assert.assertEquals(1, declarer.schemaBuffer.get(i).size());
			Assert.assertEquals("id", declarer.schemaBuffer.get(i).get(0));
			Assert.assertEquals(streamIds[i], declarer.streamIdBuffer.get(i));
			Assert.assertFalse(declarer.directBuffer.get(i).booleanValue());
		}
	}
	
	@Test
	public void testExecuteUnique() {
		IncSpout spout = new IncSpout(1);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Long(i));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		Assert.assertEquals(result, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testExecuteSkip() {
		final int skipInterval = 2 + this.r.nextInt(3);
		IncSpout spout = new IncSpout(skipInterval);
		
		System.err.println(skipInterval);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		int value = -1;
		for(int i = 0; i < 10; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Long(++value));
			
			if(i % skipInterval != skipInterval - 1) {
				result.add(attributes);
			} else {
				--value;
			}
			
			spout.nextTuple();
		}
		
		Assert.assertEquals(result, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testExecuteStepSizeUnique() {
		int stepSize = 1 + this.r.nextInt(5);
		IncSpout spout = new IncSpout(0, stepSize);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Long(i * stepSize));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		Assert.assertEquals(result, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testExecuteUniqueMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Long(i));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		for(String stream : streamIds) {
			Assert.assertEquals(result, collector.output.get(stream));
		}
	}
	
	@Test
	public void testExecuteAllEquals() {
		IncSpout spout = new IncSpout(1.0, 1);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Long(0));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		Assert.assertEquals(result, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testExecuteAllEqualsMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds, 1.0, 1);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Long(0));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		for(String stream : streamIds) {
			Assert.assertEquals(result, collector.output.get(stream));
		}
	}
	
	@Test
	public void testExecute() {
		IncSpout spout = new IncSpout(this.r.nextDouble(), 1);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		for(int i = 0; i < 50; ++i) {
			spout.nextTuple();
		}
		
		List<Object> first = collector.output.get(Utils.DEFAULT_STREAM_ID).removeFirst();
		for(List<Object> second : collector.output.get(Utils.DEFAULT_STREAM_ID)) {
			Assert.assertTrue(((Long)first.get(0)).longValue() <= ((Long)second.get(0)).longValue());
			first = second;
		}
	}
	
	@Test
	public void testExecuteMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds, this.r.nextDouble(), 1);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		for(int i = 0; i < 50; ++i) {
			spout.nextTuple();
		}
		
		for(String stream : streamIds) {
			List<Object> first = collector.output.get(stream).removeFirst();
			for(List<Object> second : collector.output.get(stream)) {
				Assert.assertTrue(((Long)first.get(0)).longValue() <= ((Long)second.get(0)).longValue());
				first = second;
			}
		}
	}
	
}
