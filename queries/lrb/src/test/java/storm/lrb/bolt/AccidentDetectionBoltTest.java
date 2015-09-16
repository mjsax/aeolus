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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import storm.lrb.tools.EntityHelper;
import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.types.PositionReport;





/**
 * 
 * @author richter
 */
@RunWith(PowerMockRunner.class)
public class AccidentDetectionBoltTest extends AbstractBoltTest {
	
	/**
	 * Test of execute method, of class AccidentDetectionBolt. Tests the size of
	 * {@link AccidentDetectionBolt#getAllAccidentCars() } after different tuples have been passed simulating the
	 * occurance of an accident, other traffic during accident and clearance of an accident.
	 */
	// TODO fix and reactivate
	@Ignore
	@Test
	public void testExecute() {
		// test recording of stopped car (with speed 0)
		GeneralTopologyContext generalContextMock = mock(GeneralTopologyContext.class);
		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("dummy"));
		when(generalContextMock.getComponentId(anyInt())).thenReturn("componentID");
		
		Fields schema = AccidentDetectionBolt.FIELDS_INCOMING;
		
		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(schema);
		int vehicleID0 = (int)(r.nextDouble() * 10000); // set max. value to increase readability
		PositionReport posReport0Stopped = EntityHelper.createPosReport(r, vehicleID0, 0, // minSpeed
			0 // maxSpeed
			);
		Tuple tuple = new TupleImpl(generalContextMock, new Values(posReport0Stopped), 1, // taskId
			null // streamID
		);
		AccidentDetectionBolt instance = new AccidentDetectionBolt();
		TestOutputCollector collector = new TestOutputCollector();
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);
		
		GeneralTopologyContext context = mock(GeneralTopologyContext.class);
		when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("dummy"));
		when(context.getComponentId(anyInt())).thenReturn("componentID");
		
		instance.prepare(new Config(), contextMock, new OutputCollector(collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		// initial setup
		instance.declareOutputFields(outputFieldsDeclarer);
		instance.execute(tuple);
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(0, instance.getAccidentsPerPosition().size());
		
		// test that a running car (with speed > 1) is not recorded (different vehicleID)
		int vehicleID1 = vehicleID0;
		while(vehicleID1 == vehicleID0) {
			vehicleID1 = (int)(r.nextDouble() * 10000); // set max. value to increase readability
		}
		PositionReport posReport1Running = EntityHelper.createPosReport(r, vehicleID1);
		tuple = new TupleImpl(generalContextMock, new Values(posReport1Running), vehicleID1, null // streamID
		);
		instance.execute(tuple);
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(0, instance.getAccidentsPerPosition().size());
		
		// test that an accident (4 consecutive pos reports with speed 0 of two
		// vehicles)
		PositionReport posReport0Stopped1 = EntityHelper.createPosReport(r, vehicleID0, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport0Stopped1), vehicleID0, null // streamID
		);
		instance.execute(tuple);
		PositionReport posReport0Stopped2 = EntityHelper.createPosReport(r, vehicleID0, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport0Stopped2), vehicleID0, null // streamID
		);
		instance.execute(tuple);
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(0, instance.getAccidentsPerPosition().size());
		PositionReport posReport0Stopped3 = EntityHelper.createPosReport(r, vehicleID0, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport0Stopped3), vehicleID0, null // streamID
		);
		instance.execute(tuple);
		// first car eventually involved in accident
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(0, instance.getAccidentsPerPosition().size());
		PositionReport posReport1Stopped0 = EntityHelper.createPosReport(r, vehicleID1, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport1Stopped0), vehicleID1, null // streamID
		);
		instance.execute(tuple);
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(2, instance.getStopInformationPerPosition().get(1).size());
		assertEquals(0, instance.getAccidentsPerPosition().size());
		PositionReport posReport1Stopped1 = EntityHelper.createPosReport(r, vehicleID1, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport1Stopped1), vehicleID1, null // streamID
		);
		instance.execute(tuple);
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(2, instance.getStopInformationPerPosition().get(1).size());
		assertEquals(0, instance.getAccidentsPerPosition().size());
		PositionReport posReport1Stopped2 = EntityHelper.createPosReport(r, vehicleID1, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport1Stopped2), vehicleID1, null // streamID
		);
		instance.execute(tuple);
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(2, instance.getStopInformationPerPosition().get(1).size());
		assertEquals(0, instance.getAccidentsPerPosition().size());
		PositionReport posReport1Stopped3 = EntityHelper.createPosReport(r, vehicleID1, 0, // minSpeed
			0 // maxSpeed
			);
		tuple = new TupleImpl(generalContextMock, new Values(posReport1Stopped3), vehicleID1, null // streamID
		);
		instance.execute(tuple);
		assertEquals(1, instance.getStopInformationPerPosition().size());
		assertEquals(2, instance.getStopInformationPerPosition().get(1).size());
		assertEquals(1, instance.getAccidentsPerPosition().size());
		
		// test that stopped car is removed from accident status collection when resumes driving
		PositionReport posReport0Running = EntityHelper.createPosReport(r, vehicleID0);
		tuple = new TupleImpl(generalContextMock, new Values(posReport0Running), vehicleID0, null);
		instance.execute(tuple);
		assertEquals(0, instance.getAccidentsPerPosition().size());
	}
}
