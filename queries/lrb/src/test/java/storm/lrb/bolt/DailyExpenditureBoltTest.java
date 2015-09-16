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

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import storm.lrb.TopologyControl;
import storm.lrb.model.DailyExpenditureRequest;
import storm.lrb.tools.Helper;
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
import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.toll.MemoryTollDataStore;
import de.hub.cs.dbis.lrb.types.AbstractLRBTuple;
import de.hub.cs.dbis.lrb.util.Constants;





/**
 * Tests {@link DailyExpenditureBolt} with mocks.
 * 
 * @author richter
 */
public class DailyExpenditureBoltTest extends AbstractBoltTest {
	
	/**
	 * Test of execute method, of class DailyExpenditureBolt.
	 */
	// TODO fix and reactivate
	@Ignore
	@Test
	public void testExecute() {
		GeneralTopologyContext generalContextMock = mock(GeneralTopologyContext.class);
		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("dummy"));
		when(generalContextMock.getComponentId(anyInt())).thenReturn("componentID");
		
		Fields schema = new Fields(TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME);
		
		when(generalContextMock.getComponentOutputFields(anyString(), anyString())).thenReturn(schema);
		TestOutputCollector collector = new TestOutputCollector();
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);
		
		GeneralTopologyContext context = mock(GeneralTopologyContext.class);
		when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("dummy"));
		when(context.getComponentId(anyInt())).thenReturn("componentID");
		DailyExpenditureBolt instance = new DailyExpenditureBolt();
		Config instanceConfig = new Config();
		instanceConfig.put(Helper.TOLL_DATA_STORE_CONF_KEY, MemoryTollDataStore.class.getName());
		instance.prepare(instanceConfig, contextMock, new OutputCollector(collector));
		OutputFieldsDeclarer outputFieldsDeclarer = Mockito.mock(OutputFieldsDeclarer.class);
		// initial setup
		instance.declareOutputFields(outputFieldsDeclarer);
		
		// test transmission of toll data/expenditure
		int vehicleIdentifierValid = 1;
		int xWay = 1;
		int queryIdentifier = 2;
		short day0 = 1;
		int expectedToll = 4748;
		instance.getDataStore().storeToll(xWay, day0, vehicleIdentifierValid, expectedToll);
		DailyExpenditureRequest dailyExpenditureRequest = new DailyExpenditureRequest(
			(short)System.currentTimeMillis(), vehicleIdentifierValid, xWay, queryIdentifier, day0);
		Tuple tuple = new TupleImpl(generalContextMock, new Values(dailyExpenditureRequest), 1, // taskId
			null // streamID
		);
		instance.execute(tuple);
		assertEquals(1, collector.acked.size());
		assertEquals(1, collector.output.size());// write only to one stream
		assertEquals(1, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		List<Object> resultTuple1 = collector.output.get(Utils.DEFAULT_STREAM_ID).get(0);
		assertEquals(AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, resultTuple1.get(0));
		assertEquals(dailyExpenditureRequest.getTime(), resultTuple1.get(1));
		// don't care about processing time
		assertEquals(queryIdentifier, resultTuple1.get(3));
		assertEquals(expectedToll, resultTuple1.get(4));
		
		// test transmission of initial toll for yet inexsting accounts
		int vehicleIdentifierInvalid = 2;
		dailyExpenditureRequest = new DailyExpenditureRequest(day0, vehicleIdentifierInvalid, xWay, queryIdentifier,
			day0);
		tuple = new TupleImpl(generalContextMock, new Values(dailyExpenditureRequest), 1, // taskId
			null // streamId
		);
		instance.execute(tuple);
		assertEquals(2, collector.acked.size());
		assertEquals(1, collector.output.size());
		assertEquals(2, collector.output.get(Utils.DEFAULT_STREAM_ID).size());
		List<Object> resultTuple2 = collector.output.get(Utils.DEFAULT_STREAM_ID).get(1);
		assertEquals(AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, resultTuple2.get(0));
		assertEquals(dailyExpenditureRequest.getTime(), resultTuple2.get(1));
		// don't care about the processing time
		assertEquals(queryIdentifier, resultTuple2.get(3));
		assertEquals(Constants.INITIAL_TOLL, resultTuple2.get(4));
		
		
	}
	
}
