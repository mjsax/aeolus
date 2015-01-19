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
package debs2013;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SensorSpout.class)
public class ParseBoltTest {
	private final static List<List<Object>> result = new LinkedList<List<Object>>();
	private final static List<Tuple> input = new LinkedList<Tuple>();
	
	private final static long seed = System.currentTimeMillis();
	private final static Random r = new Random(seed);
	
	private final static long[] sensorIDs = new long[] {4, 8, 10, 12, 13, 14, 97, 98, 47, 16, 49, 88, 19, 52, 53, 54,
												23, 24, 57, 58, 59, 28, 61, 62, 99, 100, 63, 64, 65, 66, 67, 68, 69,
												38, 71, 40, 73, 74, 75, 44, 105, 106};
	
	
	
	@BeforeClass
	public static void prepareStatic() {
		System.out.println("Test seed: " + seed);
	}
	
	
	
	@Test
	public void testExecute() throws Exception {
		GeneralTopologyContext context = mock(GeneralTopologyContext.class);
		when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("ts", "rawTuple"));
		
		ParseBolt bolt = new ParseBolt();
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		final int numberOfAttributes = 13;
		result.clear();
		for(int i = 0; i < numberOfAttributes + 2; ++i) {
			List<Object> values = new Values(new Long(sensorIDs[r.nextInt(sensorIDs.length)]), new Long(r.nextLong()),
				new Integer(r.nextInt()), new Integer(r.nextInt()), new Integer(r.nextInt()), new Integer(r.nextInt()),
				new Integer(r.nextInt()), new Integer(r.nextInt()), new Integer(r.nextInt()), new Integer(r.nextInt()),
				new Integer(r.nextInt()), new Integer(r.nextInt()), new Integer(r.nextInt()));
			
			if(i < numberOfAttributes) {
				values.set(i, new Object());
			} else if(i == numberOfAttributes + 1) {
				result.add(values);
			}
			
			List<Object> attributes = new LinkedList<Object>();
			attributes.add(values.get(1));
			
			if(i != numberOfAttributes) {
				String raw = "";
				for(Object attribute : values) {
					raw += "," + attribute;
				}
				attributes.add(raw.substring(1));
			} else {
				attributes.add(new Object());
			}
			
			Tuple tuple = new TupleImpl(context, attributes, 0, null, null);
			input.add(tuple);
			bolt.execute(tuple);
			
			if(result.size() > 0) {
				Assert.assertEquals(result, collector.output.get(ParseBolt.playerStreamId));
			} else {
				Assert.assertNull(collector.output.get(ParseBolt.playerStreamId));
			}
			Assert.assertEquals(input, collector.acked);
			Assert.assertTrue(collector.failed.size() == 0);
		}
	}
	
	@Test
	public void testDeclareOutputFields() {
		List<String> schema = new Fields("sid", "ts", "x", "y", "z", "|v|", "|a|", "vx", "vy", "vz", "ax", "ay", "az")
			.toList();
		
		ParseBolt bolt = new ParseBolt();
		TestDeclarer declarerMock = new TestDeclarer();
		bolt.declareOutputFields(declarerMock);
		
		Assert.assertTrue(declarerMock.schema.size() == 3);
		Assert.assertTrue(declarerMock.streamId.size() == 3);
		Assert.assertTrue(declarerMock.direct.size() == 3);
		
		Assert.assertTrue(declarerMock.streamId.contains(ParseBolt.playerStreamId));
		Assert.assertTrue(declarerMock.streamId.contains(ParseBolt.ballStreamId));
		Assert.assertTrue(declarerMock.streamId.contains(ParseBolt.refereeStreamId));
		
		Assert.assertEquals(schema, declarerMock.schema.get(0).toList());
		Assert.assertEquals(schema, declarerMock.schema.get(1).toList());
		Assert.assertEquals(schema, declarerMock.schema.get(2).toList());
		
		Assert.assertEquals(new Boolean(false), declarerMock.direct.get(0));
		Assert.assertEquals(new Boolean(false), declarerMock.direct.get(1));
		Assert.assertEquals(new Boolean(false), declarerMock.direct.get(2));
	}
	
}
