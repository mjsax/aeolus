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
 import backtype.storm.spout.SpoutOutputCollector;
 import backtype.storm.task.OutputCollector;
 import backtype.storm.task.TopologyContext;
 import backtype.storm.topology.OutputFieldsDeclarer;
 import backtype.storm.tuple.Fields;
 import backtype.storm.tuple.Tuple;
 import backtype.storm.tuple.TupleImpl;
 import backtype.storm.tuple.Values;
 import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
 import de.hub.cs.dbis.aeolus.testUtils.TestSpoutOutputCollector;
 import de.hub.cs.dbis.lrb.operators.FileReaderSpout;
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
 import org.junit.runner.RunWith;
 import static org.mockito.Matchers.anyString;
 import static org.mockito.Mockito.mock;
 import static org.mockito.Mockito.when;
 import org.powermock.core.classloader.annotations.PrepareForTest;
 import org.powermock.modules.junit4.PowerMockRunner;
import storm.lrb.bolt.AccidentDetectionBolt;

 /**
 *
 * @author richter
 */
 @RunWith(PowerMockRunner.class)
 @PrepareForTest(FileReaderSpout.class)
 public class AccidentDetectionBoltTest {

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
	 * Test of execute method, of class AccidentDetectionBolt.
	 */
	@Test
	@Ignore
	public void testExecute() {
		// test recording of stopped car (with speed 0)
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);

		Fields schema = AccidentDetectionBolt.FIELDS;

		when(contextMock.getComponentOutputFields(anyString(),
 anyString()))
				.thenReturn(schema);
		Tuple tuple = new TupleImpl(contextMock, new Values(0, // type position
																// report = 0
				5, // time
				172, // vehicle id
				0, // speed
				0, // xway
				0, // lane
				1, // direction
				90, // segment
				480074, // position
				-1, -1, -1, -1, -1, -1), 1, // taskId
				"streamID");
		AccidentDetectionBolt instance = new AccidentDetectionBolt(
				0 // xway
		);
		TestOutputCollector collector = new TestOutputCollector();
		instance.prepare(new Config(), contextMock, new OutputCollector(
				collector));
		assertEquals(0, instance.getStoppedCars().size());
		instance.execute(tuple);
		assertEquals(1, instance.getStoppedCars().size());
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
