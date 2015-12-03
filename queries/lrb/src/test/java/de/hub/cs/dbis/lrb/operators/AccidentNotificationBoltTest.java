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

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import storm.lrb.TopologyControl;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.types.AccidentNotification;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;





/**
 * @author mjsax
 */
public class AccidentNotificationBoltTest {
	
	private final static Integer dummyInt = new Integer(1);
	private final static Short dummyShort = new Short((short)0);
	@SuppressWarnings("boxing") private final static Short[] l = new Short[] {0, 1, 2, 3, 4};
	
	// @formatter:off
	@SuppressWarnings("boxing") private final static Short[][] PosRepAndAccs = new Short[][] {
		// Position: time vid speed xway lane direction segment position
		// -> omit vid speed xway direction position => time lane segment
		// Accident: time xway segment direction
		// -> omit xway direction => time segment
		new Short[] {1, l[0], 0},
		new Short[] {31, l[0], 0},

		new Short[] {1, 0},

		new Short[] {61, l[0], 1}, // no notification (segment already passed)
		new Short[] {91, l[0], 1},

		new Short[] {2, 2},

		new Short[] {121, l[0], 2}, // get notification
		new Short[] {151, l[0], 2},

		new Short[] {3, 2},

		new Short[] {181, l[4], 2}, // no notification (exit lane)
		new Short[] {211, l[4], 2},

	};
	// @formatter:on
	
	@SuppressWarnings("boxing")
	@Test
	public void testExecute() {
		Tuple tuple = mock(Tuple.class);
		
		OngoingStubbing<List<Object>> valueStub = when(tuple.getValues());
		
		for(Short[] input : PosRepAndAccs) {
			if(input.length == 3) {
				valueStub = valueStub.thenReturn(new PositionReport(input[0], dummyInt, dummyInt, dummyInt, input[1],
					dummyShort, input[2], dummyInt));
				
			} else {
				assert (input.length == 2);
				valueStub = valueStub.thenReturn(new AccidentTuple(input[0], dummyInt, input[1], dummyShort));
			}
		}
		
		OngoingStubbing<String> streamStub = when(tuple.getSourceStreamId());
		for(Short[] input : PosRepAndAccs) {
			if(input.length == 3) {
				streamStub = streamStub.thenReturn(TopologyControl.POSITION_REPORTS_STREAM);
				
			} else {
				assert (input.length == 2);
				streamStub = streamStub.thenReturn(Utils.DEFAULT_STREAM_ID);
			}
		}
		
		AccidentNotificationBolt bolt = new AccidentNotificationBolt();
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		for(int i = 0; i < PosRepAndAccs.length; ++i) {
			bolt.execute(tuple);
		}
		
		List<AccidentNotification> expectedResult = new ArrayList<AccidentNotification>();
		expectedResult.add(new AccidentNotification((short)121, (short)121, (short)2, dummyInt));
		
		Assert.assertEquals(1, collector.output.size());
		Assert.assertEquals(expectedResult, collector.output.get(Utils.DEFAULT_STREAM_ID));
	}
}
