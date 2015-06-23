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
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import storm.lrb.TopologyControl;
import storm.lrb.tools.Constants;





/**
 * Tests {@link LastAverageSpeedBolt} using mocks.
 *
 * @author richter
 */
public class LastAverageSpeedBoltTest {

	/**
	 * Test of execute method, of class LastAverageSpeedBolt. Tests the calculation of the average of values passed in
	 * multiple tuples with a certain minute value.
	 */
	@Test
	public void testExecute() {
		GeneralTopologyContext generalContextMock = mock(GeneralTopologyContext.class);
		when(generalContextMock.getComponentId(anyInt())).thenReturn("componentID");

		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(
			new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
				TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
				TopologyControl.CAR_COUNT_FIELD_NAME, TopologyControl.AVERAGE_SPEED_FIELD_NAME));
		LastAverageSpeedBolt instance = new LastAverageSpeedBolt();
		TestOutputCollector collector = new TestOutputCollector();
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);

		instance.prepare(new Config(), contextMock, new OutputCollector(collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		instance.declareOutputFields(outputFieldsDeclarer);

		// test last average speed calculation
		int minuteOfTuple = 5;
		int xway = 1;
		int segment = 2;
		int direction = Constants.DIRECTION_EASTBOUND;
		SegmentIdentifier segmentIdentifier = new SegmentIdentifier(xway, segment, direction);
		int carcnt = 5;
		double initialSpeedAverage = 35.3;
		Tuple tuple = new TupleImpl(generalContextMock, new Values(minuteOfTuple, xway, segmentIdentifier, direction,
			carcnt, initialSpeedAverage), 0, Utils.DEFAULT_STREAM_ID);
		instance.execute(tuple);
		assertEquals(1, collector.acked.size());
		assertEquals(1, collector.output.size());
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		List<Object> result = collector.output.get(Utils.DEFAULT_STREAM_ID).get(0);
		Integer resultXWay = (Integer)result.get(0);
		Integer resultDirection = (Integer)result.get(1);
		SegmentIdentifier resultSegmentIdentifier = (SegmentIdentifier)result.get(2);
		Integer resultCarCount = (Integer)result.get(3);
		Double resultSpeedAverage = (Double)result.get(4);
		Integer resultMinute = (Integer)result.get(5);
		assertEquals((int)xway, (int)resultXWay);
		assertEquals((int)direction, (int)resultDirection);
		assertEquals(segmentIdentifier, resultSegmentIdentifier);
		assertEquals((int)carcnt, (int)resultCarCount);
		double expectedSpeedAverage = initialSpeedAverage;
		assertEquals(expectedSpeedAverage, resultSpeedAverage, 0.0);
		int expectedMinute = minuteOfTuple + 1;
		assertEquals((int)expectedMinute, (int)resultMinute);
	}

	/**
	 * Test of calcLav method, of class LastAverageSpeedBolt. Tests refusal of illegal arguments as well as whether
	 * average is calculated correctly ignoring the latest value.
	 */
	@Test
	public void testCalcLav() {
		List<Double> speedValues = new LinkedList<Double>(Arrays.asList(4.5, 5.6, 6.7, 7.8));
		LastAverageSpeedBolt instance = new LastAverageSpeedBolt();
		// test result 0.0 if minute is 0
		double expResult = 0.0;
		int minute = 0;
		double result = instance.calcLav(speedValues, minute);
		assertEquals(expResult, result, 0.0);
		// test IllegalArgumentException
		minute = 4;
		try {
			instance.calcLav(speedValues, minute);
			fail("IllegalArgumentException expected");
		} catch(IllegalArgumentException ignore) {}
		minute = 3;
		expResult = (5.6 + 6.7 + 7.8) / 3;
		result = instance.calcLav(speedValues, minute);
		assertEquals(expResult, result, 0.0);
	}

}
