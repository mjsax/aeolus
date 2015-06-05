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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import storm.lrb.TopologyControl;
import storm.lrb.model.Accident;
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.PosReport;
import storm.lrb.tools.Constants;
import storm.lrb.tools.EntityHelper;





/**
 *
 * @author richter
 */
public class AccidentNotificationBoltTest {
	private static final Random random = new Random();

	/**
	 * Test of execute method, of class AccidentNotificationBolt.
	 */
	@Test
	public void testExecute() {

		GeneralTopologyContext generalContextMock = mock(GeneralTopologyContext.class);
		when(generalContextMock.getComponentId(anyInt())).thenReturn("componentID");

		when(generalContextMock.getComponentOutputFields(anyString(), eq(TopologyControl.POS_REPORTS_STREAM_ID) // streamId
																												// (use
			// mockito matcher because raw values are not allowed
			// together with other matcher)
			)).thenReturn(new Fields(TopologyControl.POS_REPORT_FIELD_NAME));
		when(generalContextMock.getComponentOutputFields(anyString(), eq(TopologyControl.ACCIDENT_INFO_STREAM_ID) // streamId
			)).thenReturn(new Fields(TopologyControl.ACCIDENT_INFO_FIELD_NAME));
		AccidentNotificationBolt instance = new AccidentNotificationBolt();
		TestOutputCollector collector = new TestOutputCollector();
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);

		instance.prepare(new Config(), contextMock, new OutputCollector(collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		instance.declareOutputFields(outputFieldsDeclarer);

		// test that PosReports don't cause notification if no accident has been submitted
		int vehicleIdentifier = 454;
		PosReport posReport1 = EntityHelper.createPosReport(random, vehicleIdentifier);
		Tuple tuple1 = new TupleImpl(generalContextMock, new Values(posReport1), 0, // taskId
			TopologyControl.POS_REPORTS_STREAM_ID // streamId
		);
		instance.execute(tuple1);
		PosReport posReport2 = EntityHelper.createPosReport(random, vehicleIdentifier);
		Tuple tuple2 = new TupleImpl(generalContextMock, new Values(posReport2), 0, // taskId
			TopologyControl.POS_REPORTS_STREAM_ID // streamId
		);
		instance.execute(tuple2);
		assertEquals(2, collector.acked.size());
		assertEquals(0, collector.output.size());

		// trigger accident
		int posReportAccidentSegment = 775;
		long posReportAccidentCreated = System.currentTimeMillis();
		PosReport posReportAccident = EntityHelper.createPosReport(posReportAccidentCreated, posReportAccidentSegment,
			random, vehicleIdentifier, 30, // minSpeed
			170 // maxSpeed
			);
		AccidentImmutable accident = new AccidentImmutable(posReportAccident);
		Tuple tuple3 = new TupleImpl(generalContextMock, new Values(accident), 0, // taskId
			TopologyControl.ACCIDENT_INFO_STREAM_ID // streamId
		);
		instance.execute(tuple3);
		assertEquals(3, collector.acked.size());
		assertEquals(0, collector.output.size());

		// test that accident notifications more than 4 segments upstream cause notification
		int posReport4Segment = posReportAccidentSegment - 5;
		long posReport4Created = System.currentTimeMillis();
		PosReport posReport4 = EntityHelper.createPosReport(posReport4Created, posReport4Segment, random,
			vehicleIdentifier, 20, // minSpeed
			179 // maxSpeed
			);
		Tuple tuple4 = new TupleImpl(generalContextMock, new Values(posReport4), 0, // taskId
			TopologyControl.POS_REPORTS_STREAM_ID // streamId
		);
		instance.execute(tuple4);
		assertEquals(4, collector.acked.size());
		assertEquals(0, collector.output.size());

		// test that accident notification 4 segments upstream or less are
		// submitted cause notification (failure might be related to previous
		// tests (that's not too elegant, but avoids to expose bolt internals)
		int posReport5Segment = posReportAccidentSegment - 4;
		long posReport5Created = System.currentTimeMillis();
		PosReport posReport5 = EntityHelper.createPosReport(posReport5Created, posReport5Segment, random,
			vehicleIdentifier, 20, // minSpeed
			179 // maxSpeed
			);
		Tuple tuple5 = new TupleImpl(generalContextMock, new Values(posReport5), 0, // taskId
			TopologyControl.POS_REPORTS_STREAM_ID // streamId
		);
		instance.execute(tuple5);
		assertEquals(5, collector.acked.size());
		assertEquals(1, collector.output.size());
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).get(0).size());
		assertEquals(posReport5, collector.output.get(Utils.DEFAULT_STREAM_ID).get(0).get(0));
		// expect to get the sent accident report back

		// test that there's a notification for reports for the same segment as
		// well
		int posReport6Segment = posReportAccidentSegment;
		long posReport6Created = System.currentTimeMillis();
		PosReport posReport6 = EntityHelper.createPosReport(posReport6Created, posReport6Segment, random,
			vehicleIdentifier, 20, // minSpeed
			179 // maxSpeed
			);
		Tuple tuple6 = new TupleImpl(generalContextMock, new Values(posReport6), 0, // taskId
			TopologyControl.POS_REPORTS_STREAM_ID // streamId
		);
		instance.execute(tuple6);
		assertEquals(6, collector.acked.size());
		// expect no changes to the previous result
		assertEquals(1, collector.output.size());
		assertEquals(2, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).get(0).size());
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).get(1).size());
		assertEquals(posReport5, collector.output.get(Utils.DEFAULT_STREAM_ID).get(0).get(0));
		assertEquals(posReport6, collector.output.get(Utils.DEFAULT_STREAM_ID).get(1).get(0));

		// @TODO: test that only one notification is sent if more than one
		// accident is on the 4 upcoming segments

		// @TODO: test skip of emission if vehicle on accident lane
	}

}
