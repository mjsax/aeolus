package de.hub.cs.dbis.aeolus.testUtils;

/*
 * #%L
 * testUtils
 * $Id:$
 * $HeadURL:$
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
public class IncSpoutTest {
	private final static long seed = System.currentTimeMillis();
	private final static Random r = new Random(seed);
	
	@BeforeClass
	public static void prepareStatic() {
		System.out.println("Test seed: " + seed);
	}
	
	@Test
	public void testDeclareOutputFields() {
		IncSpout spout = new IncSpout();
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertTrue(declarer.schema.size() == 1);
		Assert.assertTrue(declarer.schema.get(0).size() == 1);
		Assert.assertTrue(declarer.schema.get(0).get(0).equals("id"));
		Assert.assertTrue(declarer.streamId.size() == 1);
		Assert.assertTrue(declarer.streamId.get(0).equals(Utils.DEFAULT_STREAM_ID));
		Assert.assertTrue(declarer.direct.size() == 1);
		Assert.assertTrue(declarer.direct.get(0).booleanValue() == false);
	}
	
	@Test
	public void testDeclareOutputFieldsMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds);
		
		TestDeclarer declarer = new TestDeclarer();
		spout.declareOutputFields(declarer);
		
		Assert.assertTrue(declarer.schema.size() == streamIds.length);
		Assert.assertTrue(declarer.streamId.size() == streamIds.length);
		Assert.assertTrue(declarer.direct.size() == streamIds.length);
		
		for(int i = 0; i < streamIds.length; ++i) {
			Assert.assertTrue(declarer.schema.get(i).size() == 1);
			Assert.assertTrue(declarer.schema.get(i).get(0).equals("id"));
			Assert.assertTrue(declarer.streamId.get(i).equals(streamIds[i]));
			Assert.assertTrue(declarer.direct.get(i).booleanValue() == false);
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
			attributes.add(new Long(i));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		Assert.assertEquals(result, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testExecuteStepSizeUnique() {
		int stepSize = 1 + r.nextInt(5);
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
		IncSpout spout = new IncSpout(r.nextDouble(), 1);
		
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
		IncSpout spout = new IncSpout(streamIds, r.nextDouble(), 1);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 50; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Integer(0));
			result.add(attributes);
			
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
