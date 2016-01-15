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
package de.hub.cs.dbis.lrb.operators;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.modules.junit4.PowerMockRunner;

import storm.lrb.TopologyControl;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.types.AccountBalanceRequest;
import de.hub.cs.dbis.lrb.types.DailyExpenditureRequest;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.TravelTimeRequest;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
public class DispatcherBoltTest {
	
	@SuppressWarnings("boxing")
	@Test
	public void testExecute() {
		List<Values> expectedResult = new ArrayList<Values>();
		expectedResult.add(new PositionReport((short)8400, 32, 0, 0, (short)1, (short)0, (short)2, 11559));
		expectedResult.add(new AccountBalanceRequest((short)8410, 106483, 42));
		expectedResult.add(new DailyExpenditureRequest((short)8420, 104954, 4, 43, (short)3));
		expectedResult.add(new TravelTimeRequest((short)8430, 104596, 3, 44, (short)5, (short)8, (short)4, (short)67));
		
		Tuple tuple = mock(Tuple.class);
		// Type, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos, QID, S_init, S_end, DOW, TOD, Day
		OngoingStubbing<String> tupleStub1 = when(tuple.getString(1));
		tupleStub1 = tupleStub1.thenReturn("0,8400,32,0,0,1,0,2,11559,-1,-1,-1,-1,-1,-1");
		tupleStub1 = tupleStub1.thenReturn("2,8410,106483,-1,-1,-1,-1,-1,-1,42,-1,-1,-1,-1,-1");
		tupleStub1 = tupleStub1.thenReturn("3,8420,104954,-1,4,-1,v,-1,-1,43,-1,-1,-1,-1,3");
		tupleStub1 = tupleStub1.thenReturn("4,8430,104596,-1,3,-1,-1,-1,-1,44,5,8,4,67,-1");
		tupleStub1.thenReturn(null);
		
		OngoingStubbing<Long> tupleStub2 = when(tuple.getLong(0));
		tupleStub2 = tupleStub2.thenReturn(8400L);
		tupleStub2 = tupleStub2.thenReturn(8410L);
		tupleStub2 = tupleStub2.thenReturn(8420L);
		tupleStub2 = tupleStub2.thenReturn(8430L);
		tupleStub2.thenReturn(null);
		
		
		
		DispatcherBolt bolt = new DispatcherBolt();
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		for(int i = 0; i < 4; ++i) {
			bolt.execute(tuple);
		}
		
		Assert.assertEquals(4, collector.output.size());
		Assert.assertEquals(1, collector.output.get(TopologyControl.POSITION_REPORTS_STREAM_ID).size());
		Assert.assertEquals(1, collector.output.get(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID).size());
		Assert.assertEquals(1, collector.output.get(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID).size());
		Assert.assertEquals(1, collector.output.get(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID).size());
		
		Assert.assertEquals(expectedResult.get(0), collector.output.get(TopologyControl.POSITION_REPORTS_STREAM_ID)
			.get(0));
		Assert.assertEquals(expectedResult.get(1),
			collector.output.get(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID).get(0));
		Assert.assertEquals(expectedResult.get(2),
			collector.output.get(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID).get(0));
		Assert.assertEquals(expectedResult.get(3), collector.output.get(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID)
			.get(0));
		
	}
	
	@Test
	public void testDeclareOutputFields() {
		DispatcherBolt bolt = new DispatcherBolt();
		
		TestDeclarer declarer = new TestDeclarer();
		bolt.declareOutputFields(declarer);
		
		final int numberOfOutputStreams = 4;
		
		Assert.assertEquals(numberOfOutputStreams, declarer.streamIdBuffer.size());
		Assert.assertEquals(numberOfOutputStreams, declarer.schemaBuffer.size());
		Assert.assertEquals(numberOfOutputStreams, declarer.directBuffer.size());
		
		HashMap<String, Fields> expectedStreams = new HashMap<String, Fields>();
		expectedStreams.put(TopologyControl.POSITION_REPORTS_STREAM_ID, new Fields(TopologyControl.TYPE_FIELD_NAME,
			TopologyControl.TIME_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.SPEED_FIELD_NAME,
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.LANE_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.POSITION_FIELD_NAME));
		expectedStreams.put(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, new Fields(
			TopologyControl.TYPE_FIELD_NAME, TopologyControl.TIME_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME,
			TopologyControl.QUERY_ID_FIELD_NAME));
		expectedStreams.put(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, new Fields(
			TopologyControl.TYPE_FIELD_NAME, TopologyControl.TIME_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME,
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.QUERY_ID_FIELD_NAME, TopologyControl.DAY_FIELD_NAME));
		expectedStreams.put(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, new Fields(TopologyControl.TYPE_FIELD_NAME,
			TopologyControl.TIME_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
			TopologyControl.QUERY_ID_FIELD_NAME, TopologyControl.START_SEGMENT_FIELD_NAME,
			TopologyControl.END_SEGMENT_FIELD_NAME, TopologyControl.DAY_OF_WEEK_FIELD_NAME,
			TopologyControl.TIME_OF_DAY_FIELD_NAME));
		
		Assert.assertEquals(expectedStreams.keySet(), new HashSet<String>(declarer.streamIdBuffer));
		
		for(int i = 0; i < numberOfOutputStreams; ++i) {
			Assert.assertEquals(expectedStreams.get(declarer.streamIdBuffer.get(i)).toList(), declarer.schemaBuffer
				.get(i).toList());
			Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(i));
		}
	}
	
}
