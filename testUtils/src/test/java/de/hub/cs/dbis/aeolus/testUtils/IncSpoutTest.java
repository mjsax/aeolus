package de.hub.cs.dbis.aeolus.testUtils;

/*
 * #%L
 * testUtils
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
		
		Assert.assertEquals(1, declarer.schema.size());
		Assert.assertEquals(1, declarer.schema.get(0).size());
		Assert.assertEquals("id", declarer.schema.get(0).get(0));
		Assert.assertEquals(1, declarer.streamId.size());
		Assert.assertEquals(Utils.DEFAULT_STREAM_ID, declarer.streamId.get(0));
		Assert.assertEquals(1, declarer.direct.size());
		Assert.assertEquals(false, declarer.direct.get(0));
	}
	
	@Test
	public void testDeclareOutputFieldsMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertEquals(streamIds.length, declarer.schema.size());
		Assert.assertEquals(streamIds.length, declarer.streamId.size());
		Assert.assertEquals(streamIds.length, declarer.direct.size());
		
		for(int i = 0; i < streamIds.length; ++i) {
			Assert.assertEquals(1, declarer.schema.get(i).size());
			Assert.assertEquals("id", declarer.schema.get(i).get(0));
			Assert.assertEquals(streamIds[i], declarer.streamId.get(i));
			Assert.assertEquals(false, declarer.direct.get(i));
		}
	}
	
	@Test
	public void testExecuteUnique() {
		IncSpout spout = new IncSpout();
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add((long) i);
			result.add(attributes);
			
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
			attributes.add((long) i * stepSize);
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
			attributes.add((long) i);
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
			attributes.add((long) 0);
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
			attributes.add((long) 0);
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
			Assert.assertTrue(((Long)first.get(0)) <= ((Long)second.get(0)));
			first = second;
		}
	}
	
	@Test
	public void testExecuteMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds, this.r.nextDouble(), 1);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 50; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(0);
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		for(String stream : streamIds) {
			List<Object> first = collector.output.get(stream).removeFirst();
			for(List<Object> second : collector.output.get(stream)) {
				Assert.assertTrue(((Long)first.get(0)) <= ((Long)second.get(0)));
				first = second;
			}
		}
	}
	
}
