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
import static org.junit.Assert.assertTrue;
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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import storm.lrb.TopologyControl;
import storm.lrb.model.AccountBalance;
import storm.lrb.model.AccountBalanceRequest;
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
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.types.PositionReport;





/**
 * 
 * @author richter
 */
public class AccountBalanceBoltTest {
	private static final Random random = new Random();
	
	public AccountBalanceBoltTest() {}
	
	@BeforeClass
	public static void setUpClass() {}
	
	@AfterClass
	public static void tearDownClass() {}
	
	@Before
	public void setUp() {}
	
	@After
	public void tearDown() {}
	
	/**
	 * Test of execute method, of class AccountBalanceBolt.
	 */
	// TODO fix and reactivate
	@Ignore
	@Test
	public void testExecute() {
		// test recording of stopped car (with speed 0)
		GeneralTopologyContext generalContextMock = mock(GeneralTopologyContext.class);
		when(generalContextMock.getComponentId(anyInt())).thenReturn("componentID");
		
		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(
			new Fields(TopologyControl.ACCOUNT_BALANCE_REQUEST_FIELD_NAME));
		AccountBalanceBolt instance = new AccountBalanceBolt();
		TestOutputCollector collector = new TestOutputCollector();
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);
		
		instance.prepare(new Config(), contextMock, new OutputCollector(collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		instance.declareOutputFields(outputFieldsDeclarer);
		
		// test processing of toll assessment stream
		int queryIdentifier = 1;
		int expectedTollTime = 1;
		AccountBalance expResult = new AccountBalance((short)System.currentTimeMillis(), queryIdentifier, 0, // balance
			expectedTollTime, (short)System.currentTimeMillis()); // create before execute for
		// timestamp comparison
		int vehicleID0 = (int)(random.nextDouble() * 10000); // set max. value to increase readability
		int xWay = 1;
		int assessedToll = 0;
		PositionReport posReport0Stopped = EntityHelper.createPosReport(random, vehicleID0, 0, // minSpeed
			0 // maxSpeed
			);
		AccountBalanceRequest accountBalanceRequest = new AccountBalanceRequest((short)System.currentTimeMillis(),
			vehicleID0, queryIdentifier);
		Tuple tuple = new TupleImpl(generalContextMock, new Values(accountBalanceRequest), 1, // taskId
			TopologyControl.TOLL_ASSESSMENT_STREAM_ID // streamID
		);
		instance.execute(tuple);
		assertEquals(1, collector.acked.size());
		AccountBalance result = (AccountBalance)collector.output.get("default").get(0);
		assertTrue(expResult.getTime() <= result.getTime());
		assertEquals(0, result.getBalance());
		assertEquals(queryIdentifier, result.getQueryIdentifier());
		// assertTrue(expectedTollTime <= result.getTollTime()); //@TODO: reactivate
		
		// test processing of account balance request stream
		generalContextMock = mock(GeneralTopologyContext.class);
		when(generalContextMock.getComponentId(anyInt())).thenReturn("componentID");
		
		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(
			AccountBalanceBolt.FIELDS_INCOMING_TOLL_ASSESSMENT);
		instance = new AccountBalanceBolt();
		collector = new TestOutputCollector();
		taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);
		
		instance.prepare(new Config(), contextMock, new OutputCollector(collector));
		outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		instance.declareOutputFields(outputFieldsDeclarer);
		
		tuple = new TupleImpl(generalContextMock, new Values(vehicleID0, xWay, assessedToll, posReport0Stopped), 1, // taskId
			TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID // streamID
		);
		instance.execute(tuple);
		assertEquals(1, collector.acked.size());
	}
	
}
