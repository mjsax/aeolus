package storm.lrb.bolt;

/*
 * #%L
 * lrb
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

 import backtype.storm.Config;
import backtype.storm.task.GeneralTopologyContext;
 import backtype.storm.task.OutputCollector;
 import backtype.storm.task.TopologyContext;
 import backtype.storm.topology.OutputFieldsDeclarer;
 import backtype.storm.tuple.Fields;
 import backtype.storm.tuple.Tuple;
 import backtype.storm.tuple.TupleImpl;
 import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.testUtils.MockHelper;
 import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import java.util.Random;
 import org.junit.After;
 import org.junit.AfterClass;
 import org.junit.Before;
 import org.junit.BeforeClass;
 import org.junit.Test;
 import static org.junit.Assert.*;
 import org.junit.Ignore;
 import org.junit.runner.RunWith;
 import static org.mockito.Matchers.anyString;
import org.mockito.Mockito;
 import static org.mockito.Mockito.when;
 import org.powermock.modules.junit4.PowerMockRunner;
import storm.lrb.model.PosReport;
import storm.lrb.tools.EntityHelper;

 /**
 *
 * @author richter
 */
 @RunWith(PowerMockRunner.class)
 public class AccidentDetectionBoltTest {
	 private static final Random random = new Random();

	public AccidentDetectionBoltTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	/**
	 * Test of execute method, of class AccidentDetectionBolt. Tests the size of {@link AccidentDetectionBolt#getAllAccidentCars() } after different tuples have been passed simulating the occurance of an accident, other traffic during accident and clearance of an accident.
	 */
	@Test
	@Ignore
	public void testExecute() {
		// test recording of stopped car (with speed 0)
		GeneralTopologyContext generalContextMock = MockHelper.createGeneralTopologyContextMock();

		Fields schema = AccidentDetectionBolt.FIELDS_INCOMING;

		when(generalContextMock.getComponentOutputFields(anyString(),
 anyString()))
				.thenReturn(schema);
		int vehicleID0 = (int) (random.nextDouble()*10000); //set max. value to increase readability
		PosReport posReport0Stopped = EntityHelper.createPosReport(random, vehicleID0, 
				0, //minSpeed
				0 //maxSpeed
		);
		Tuple tuple = new TupleImpl(generalContextMock, new Values(
				posReport0Stopped), 
				1, // taskId
				null //streamID
		);
		AccidentDetectionBolt instance = new AccidentDetectionBolt(
				0 // xway
		);
		TestOutputCollector collector = new TestOutputCollector();
		TopologyContext contextMock = MockHelper.createTopologyContextMock();
		instance.prepare(new Config(), contextMock, new OutputCollector(
				collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		instance.declareOutputFields(outputFieldsDeclarer);
		assertEquals(0, instance.getAllAccidentCars().size());
		instance.execute(tuple);
		assertEquals(1, instance.getAllAccidentCars().size());
		// test that a running car (with speed > 1) is not recorded (different vehicleID)
		int vehicleID1 = (int) (random.nextDouble()*10000); //set max. value to increase readability
		PosReport posReport1Running = EntityHelper.createPosReport(random, vehicleID1);
		tuple = new TupleImpl(generalContextMock, new Values(posReport1Running), vehicleID1, null //streamID
		);
		instance.execute(tuple);
		assertEquals(1, instance.getAllAccidentCars().size());
		// test that stopped car is removed from accident status collection when resumes driving
		PosReport posReport0Running = EntityHelper.createPosReport(random, vehicleID0);
		tuple = new TupleImpl(generalContextMock, new Values(posReport0Running), vehicleID0, null);
		instance.execute(tuple);
		assertEquals(0, instance.getAllAccidentCars().size());
	}

	/**
	 * Test of declareOutputFields method, of class AccidentDetectionBolt.
	 */
	@Test
	@Ignore
	public void testDeclareOutputFields() {
		System.out.println("declareOutputFields");
		OutputFieldsDeclarer declarer = null;
		AccidentDetectionBolt instance = null;
		instance.declareOutputFields(declarer);
		// TODO review the generated test code and remove the default call to
		// fail.
		fail("The test case is a prototype.");
	}

 }
