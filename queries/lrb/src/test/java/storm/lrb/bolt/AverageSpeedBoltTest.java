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
/*
 * Copyright 2015 Humboldt-Universität zu Berlin.
 *
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
 */
package storm.lrb.bolt;

import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import storm.lrb.TopologyControl;
import storm.lrb.model.AccountBalance;
import storm.lrb.model.AccountBalanceRequest;
import storm.lrb.model.PosReport;
import storm.lrb.tools.EntityHelper;
import storm.lrb.tools.StopWatch;





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
			new Fields(TopologyControl.POS_REPORT_FIELD_NAME));
		AverageSpeedBolt instance = new AverageSpeedBolt(1 // xWay
		);
		TestOutputCollector collector = new TestOutputCollector();
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);
		
		instance.prepare(new Config(), contextMock, new OutputCollector(collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		instance.declareOutputFields(outputFieldsDeclarer);
		
		// test processing of position reports
		int vehicleID0 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		long time0 = 6 * 60; // should be a full minute //@TODO: handle case where
		// invalid times are specified and test planned failure
		int segment = 5854;
		PosReport posReport0Stopped = EntityHelper.createPosReport(time0, segment, random, vehicleID0, 0, // minSpeed
			0 // maxSpeed
			);
		Tuple tuple = new TupleImpl(generalContextMock, new Values(posReport0Stopped), 1, // taskId
			"default" // streamID
		);
		instance.execute(tuple);
		assertEquals(1, collector.acked.size());
		assertEquals(0, collector.output.size()); // one tuple doesn't trigger
		// emission
		
		int vehicleID1 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		long time1 = time0 + 60;
		PosReport posReport1Stopped = EntityHelper.createPosReport(time1, segment, random, vehicleID1, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport1Stopped), 1, // taskId
			"default" // streamID
		);
		instance.execute(tuple);
		assertEquals(2, collector.acked.size());
		assertEquals(1, collector.output.size()); // should write to one stream
		// only
		assertEquals(1, collector.output.get(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID).size()); // second tuple with
																									// another time
																									// should have
																									// triggered
																									// emission
		// after one minute
		
		List<Object> result = collector.output.get(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID).get(0);
		Integer resultXWay = (Integer)result.get(0);
		SegmentIdentifier resultSegmentIdentifier = (SegmentIdentifier)result.get(1);
		Integer resultCarCount = (Integer)result.get(2);
		Double resultAverageSpeed = (Double)result.get(3);
		Long resultMinute = (Long)result.get(4);
		assertEquals((int)1, (int)resultXWay); // @TODO: suppress warning
		assertEquals(posReport1Stopped.getSegmentIdentifier(), resultSegmentIdentifier);
		assertEquals((int)1, (int)resultCarCount);
		assertEquals(0, resultAverageSpeed, 0.0);
		assertEquals((long)time1, (long)resultMinute * 60);
		
		// three cars
		int speed2 = 33;
		int speed3 = 54;
		int speed4 = 177;
		long time2 = time1 + 60;
		int vehicleID2 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		int vehicleID3 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		int vehicleID4 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		PosReport posReport2Running = EntityHelper.createPosReport(time1, segment, random, vehicleID2, speed2, // minSpeed
			speed2 // maxSpeed
			);
		Tuple tuple2 = new TupleImpl(generalContextMock, new Values(posReport2Running), 1, // taskId
			"default" // streamID
		);
		PosReport posReport3Running = EntityHelper.createPosReport(time1, segment, random, vehicleID3, speed3, // minSpeed
			speed3 // maxSpeed
			);
		Tuple tuple3 = new TupleImpl(generalContextMock, new Values(posReport3Running), 1, // taskId
			"default" // streamID
		);
		PosReport posReport4Running = EntityHelper.createPosReport(time2, segment, random, vehicleID4, speed4, // minSpeed
			speed4 // maxSpeed
			);
		Tuple tuple4 = new TupleImpl(generalContextMock, new Values(posReport4Running), 1, // taskId
			"default" // streamID
		);
		instance.execute(tuple2);
		instance.execute(tuple3);
		instance.execute(tuple4);
		assertEquals(5, collector.acked.size());
		assertEquals(1, collector.output.size()); // one stream only
		assertEquals(2, collector.output.get(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID).size());
		result = collector.output.get(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID).get(1);
		resultXWay = (Integer)result.get(0);
		resultSegmentIdentifier = (SegmentIdentifier)result.get(1);
		resultCarCount = (Integer)result.get(2);
		resultAverageSpeed = (Double)result.get(3);
		resultMinute = (Long)result.get(4);
		assertEquals((int)1, (int)resultXWay); // @TODO: suppress warning
		assertEquals(posReport1Stopped.getSegmentIdentifier(), resultSegmentIdentifier);
		assertEquals((int)3, (int)resultCarCount);
		assertEquals((0 + speed2 + speed3) / 3, resultAverageSpeed, 0.0); // average update only occurs after emission
		assertEquals((long)time2, (long)resultMinute * 60);
	}
	
}
