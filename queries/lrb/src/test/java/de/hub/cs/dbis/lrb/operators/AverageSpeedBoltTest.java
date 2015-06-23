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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import storm.lrb.TopologyControl;
import storm.lrb.bolt.SegmentIdentifier;
import storm.lrb.tools.EntityHelper;
import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.datatypes.PositionReport;





/**
 * 
 * @author richter
 */
public class AverageSpeedBoltTest {
	
	private static final Random random = new Random();
	
	public AverageSpeedBoltTest() {}
	
	@BeforeClass
	public static void setUpClass() {}
	
	@AfterClass
	public static void tearDownClass() {}
	
	@Before
	public void setUp() {}
	
	@After
	public void tearDown() {}
	
	/**
	 * Test of execute method, of class AverageSpeedBolt. Test one tuple with speed 0 for a minute x and then 1 tuple
	 * with speed 0 with minute x+1 to test emission (should occur). Then test 2 tuples with speed > 0 with minute x+1
	 * and one with speed > 0 with minute x+1 to test emission with average speed of all tuple.
	 */
	@Test
	public void testExecute() {
		// test recording of stopped car (with speed 0)
		GeneralTopologyContext generalContextMock = mock(GeneralTopologyContext.class);
		when(generalContextMock.getComponentId(anyInt())).thenReturn("componentID");
		
		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(
			new Fields(TopologyControl.TYPE_FIELD_NAME, TopologyControl.TIMESTAMP_FIELD_NAME,
				TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.SPEED_FIELD_NAME,
				TopologyControl.XWAY_FIELD_NAME, TopologyControl.LANE_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
				TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.POSITION_FIELD_NAME));
		AverageVehicleSpeedBolt instance = new AverageVehicleSpeedBolt();
		TestOutputCollector collector = new TestOutputCollector();
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(new Integer(0));
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(new Integer(contextMock.getThisTaskIndex())).thenReturn(new Integer(0));
		
		instance.prepare(new Config(), contextMock, new OutputCollector(collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		instance.declareOutputFields(outputFieldsDeclarer);
		
		// test processing of position reports
		int vehicleID0 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		long time0 = 6 * 60; // should be a full minute //@TODO: handle case where
		// invalid times are specified and test planned failure
		short segment = 5854;
		PositionReport posReport0Stopped = EntityHelper.createPosReport(new Long(time0), new Short(segment), random,
			new Integer(vehicleID0), 0, // minSpeed
			0 // maxSpeed
			);
		Tuple tuple = new TupleImpl(generalContextMock, posReport0Stopped, 1, // taskId
			Utils.DEFAULT_STREAM_ID // streamID
		);
		instance.execute(tuple);
		assertEquals(1, collector.acked.size());
		assertEquals(0, collector.output.size()); // one tuple doesn't trigger
		// emission
		
		int vehicleID1;
		do {
			vehicleID1 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		} while(vehicleID1 == vehicleID0);
		long time1 = time0 + 60;
		PositionReport posReport1Stopped = EntityHelper.createPosReport(new Long(time1), new Short(segment), random,
			new Integer(vehicleID1), 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, posReport1Stopped, 1, // taskId
			"default" // streamID
		);
		instance.execute(tuple);
		assertEquals(2, collector.acked.size());
		assertEquals(1, collector.output.size()); // should write to one stream only
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).size()); // second tuple with
																				// another time
																				// should have
																				// triggered
																				// emission
		// after one minute
		
		List<Object> result = collector.output.get(Utils.DEFAULT_STREAM_ID).get(0);
		SegmentIdentifier resultSegmentIdentifier = new SegmentIdentifier((Integer)result.get(2), (Short)result.get(3),
			(Short)result.get(4));
		// Integer resultCarCount = (Integer)result.get(1);
		int resultAverageSpeed = ((Integer)result.get(5)).intValue();
		short resultMinute = ((Short)result.get(1)).shortValue();
		assertEquals(new SegmentIdentifier(posReport1Stopped), resultSegmentIdentifier);
		// assertEquals(1, (int)resultCarCount);// @TODO: suppress warning
		assertEquals(0, resultAverageSpeed);
		assertEquals(time1, resultMinute * 60);
		
		// three cars
		int speed2 = 33;
		int speed3 = 54;
		int speed4 = 177;
		long time2 = time1 + 60;
		int vehicleID2;
		do {
			vehicleID2 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		} while(vehicleID2 == vehicleID1 || vehicleID2 == vehicleID0);
		
		PositionReport posReport2Running = EntityHelper.createPosReport(new Long(time1), new Short(segment), random,
			new Integer(vehicleID1), speed2, // minSpeed
			speed2 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, posReport2Running, 1, // taskId
			Utils.DEFAULT_STREAM_ID // streamID
		);
		instance.execute(tuple);
		
		PositionReport posReport3Running = EntityHelper.createPosReport(new Long(time1), new Short(segment), random,
			new Integer(vehicleID1), speed3, // minSpeed
			speed3 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, posReport3Running, 1, // taskId
			Utils.DEFAULT_STREAM_ID // streamID
		);
		instance.execute(tuple);
		
		PositionReport posReport4Running = EntityHelper.createPosReport(new Long(time2), new Short(segment), random,
			new Integer(vehicleID2), speed4, // minSpeed
			speed4 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, posReport4Running, 1, // taskId
			Utils.DEFAULT_STREAM_ID // streamID
		);
		instance.execute(tuple);
		
		assertEquals(5, collector.acked.size());
		assertEquals(1, collector.output.size()); // one stream only
		assertEquals(2, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		result = collector.output.get(Utils.DEFAULT_STREAM_ID).get(1);
		resultSegmentIdentifier = new SegmentIdentifier((Integer)result.get(2), (Short)result.get(3),
			(Short)result.get(4));
		// resultCarCount = (Integer)result.get(1);
		resultAverageSpeed = ((Integer)result.get(5)).intValue();
		resultMinute = ((Short)result.get(1)).shortValue();
		assertEquals(new SegmentIdentifier(posReport1Stopped), resultSegmentIdentifier);
		// assertEquals(3, (int)resultCarCount);// @TODO: suppress warning
		assertEquals((0 + speed2 + speed3) / 3, resultAverageSpeed); // average update only occurs after emission
		assertEquals(time2, resultMinute * 60);
	}
}
