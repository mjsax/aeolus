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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
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
	public void testExecute() {
		IncSpout spout = new IncSpout();
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 0; i < 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Integer(1 + i));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		Assert.assertEquals(result, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
	
	@Test
	public void testExecuteMultipleStreams() {
		String[] streamIds = new String[] {Utils.DEFAULT_STREAM_ID, "myStreamId"};
		IncSpout spout = new IncSpout(streamIds);
		
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		spout.open(null, null, new SpoutOutputCollector(collector));
		
		List<List<Object>> result = new LinkedList<List<Object>>();
		
		for(int i = 1; i <= 5; ++i) {
			ArrayList<Object> attributes = new ArrayList<Object>();
			attributes.add(new Integer(i));
			result.add(attributes);
			
			spout.nextTuple();
		}
		
		for(String stream : streamIds) {
			Assert.assertEquals(result, collector.output.get(stream));
		}
	}
	
}
