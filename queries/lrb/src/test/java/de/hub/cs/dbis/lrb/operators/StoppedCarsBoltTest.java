/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.StoppedCarTuple;
import de.hub.cs.dbis.lrb.util.Constants;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
public class StoppedCarsBoltTest {
	private final static int exit = Constants.EXIT_LANE;
	
	// @formatter:off
	// position size = 528000 with pos = [0..517999]
	@SuppressWarnings("boxing") private static Integer[][][] timeAndPositions = new Integer[][][] {
		// vehicle 1
		new Integer[][] {new Integer[] {0, 1, 50101},
						 new Integer[] {0, 31, 50102},
						 new Integer[] {0, 61, 50102},
						 new Integer[] {0, 91, 50102},
						 new Integer[] {0, 121, 50102}, // stop v1
						 new Integer[] {0, 151, 50103}, // move v1
						 new Integer[] {0, 181, 50103},
						 new Integer[] {0, 211, 50103},
						 new Integer[] {0, 241, 50103}, // stop v1 -> accident m=5
						 new Integer[] {0, 271, 50103}, // stop v1
						 new Integer[] {0, 301, 50105}, // move v1
						 new Integer[] {0, 331, 50105},
						 new Integer[] {0, 361, 50105},
						 new Integer[] {0, 391, 50105}, // stop v1 -> accident m=7
		},
		// vehicle 2
		new Integer[][] {new Integer[] {0, 5, 50100},
						 new Integer[] {0, 35, 50101},
						 new Integer[] {0, 65, 50102},
						 new Integer[] {0, 95, 50102},
						 new Integer[] {0, 125, 50102},
						 new Integer[] {0, 155, 50102}, // stop v2
						 new Integer[] {0, 185, 50103}, // move v2
						 new Integer[] {0, 215, 50103},
						 new Integer[] {0, 245, 50105},
						 new Integer[] {0, 275, 50105},
						 new Integer[] {0, 305, 50105},
						 new Integer[] {0, 335, 50105}, // stop v2 -> accident m=6
						 new Integer[] {0, 365, 50105}, // stop v2 -> accident m=7
						 new Integer[] {0, 395, 50106}, // move v2
		},
		// vehicle 3
		new Integer[][] {new Integer[] {0, 10, 50105},
						 new Integer[] {0, 40, 50105},
						 new Integer[] {0, 70, 50105},
						 new Integer[] {0, 100, 50105}, // stop v3
						 new Integer[] {0, 130, 50105}, // stop v3
						 new Integer[] {0, 160, 50105}, // stop v3
						 new Integer[] {0, 190, 50105}, // stop v3
						 new Integer[] {0, 220, 50105}, // stop v3
						 new Integer[] {0, 250, 50105}, // stop v3
						 new Integer[] {0, 280, 50105}, // stop v3
						 new Integer[] {0, 310, 50105}, // stop v3
						 new Integer[] {0, 340, 50105}, // stop v3 -> accident m=6
						 new Integer[] {0, 370, 50105}, // stop v3 -> accident m=7
						 new Integer[] {0, 400, 50106}, // move v3
		},
		// vehicle 4 (on exit lane -- short not trigger accident)
		new Integer[][] {new Integer[] {exit, 5, 50102},
						 new Integer[] {exit, 35, 50102},
						 new Integer[] {exit, 65, 50102},
						 new Integer[] {exit, 95, 50102}, // stop v4
						 new Integer[] {exit, 125, 50102}, // stop v4
						 new Integer[] {exit, 155, 50103}, // move v4
						 new Integer[] {exit, 185, 50103},
						 new Integer[] {exit, 215, 50103},
						 new Integer[] {exit, 245, 50103}, // stop v4
						 new Integer[] {exit, 275, 50105}, // move v4
						 new Integer[] {exit, 305, 50105},
						 new Integer[] {exit, 335, 50105},
						 new Integer[] {exit, 365, 50105}, // stop v4
						 new Integer[] {exit, 395, 50105}, // stop v4
		}
	};
	// @formatter:on
	
	@SuppressWarnings("boxing")
	@Test
	public void testExecute() {
		Tuple tuple = mock(Tuple.class);
		when(tuple.getSourceStreamId()).thenReturn("streamId");
		OngoingStubbing<List<Object>> tupleValueStub = when(tuple.getValues());
		
		final int numberOfVehicles = timeAndPositions.length;
		final int numberOfReporst = timeAndPositions[0].length;
		
		for(int m = 0; m < numberOfReporst; ++m) {
			for(int vid = 1; vid <= numberOfVehicles; ++vid) {
				PositionReport pr = new PositionReport(timeAndPositions[vid - 1][m][1].shortValue(), vid, 0, 0,
					timeAndPositions[vid - 1][m][0].shortValue(), Constants.EASTBOUND,
					(short)(timeAndPositions[vid - 1][m][2].intValue() / 5280), timeAndPositions[vid - 1][m][2]);
				
				tupleValueStub = tupleValueStub.thenReturn(pr);
			}
		}
		tupleValueStub.thenReturn(null);
		
		StoppedCarsBolt bolt = new StoppedCarsBolt();
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		for(int i = 0; i < numberOfReporst * numberOfVehicles; ++i) {
			bolt.execute(tuple);
		}
		
		List<Values> expectedFlushs = new ArrayList<Values>();
		expectedFlushs.add(new Values((short)1));
		expectedFlushs.add(new Values((short)61));
		expectedFlushs.add(new Values((short)121));
		expectedFlushs.add(new Values((short)181));
		expectedFlushs.add(new Values((short)241));
		expectedFlushs.add(new Values((short)301));
		expectedFlushs.add(new Values((short)361));
		
		List<StoppedCarTuple> expectedResult = new ArrayList<StoppedCarTuple>();
		expectedResult.add(new StoppedCarTuple(3, (short)100, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(1, (short)121, 0, (short)0, 50102, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)130, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(-1, (short)151, 0, (short)0, 50102, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(2, (short)155, 0, (short)0, 50102, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)160, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(-2, (short)185, 0, (short)0, 50102, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)190, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)220, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(1, (short)241, 0, (short)0, 50103, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)250, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(1, (short)271, 0, (short)0, 50103, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)280, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(-1, (short)301, 0, (short)0, 50103, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)310, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(2, (short)335, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)340, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(2, (short)365, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(3, (short)370, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(1, (short)391, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(-2, (short)395, 0, (short)0, 50105, Constants.EASTBOUND));
		expectedResult.add(new StoppedCarTuple(-3, (short)400, 0, (short)0, 50105, Constants.EASTBOUND));
		
		Assert.assertEquals(2, collector.output.size());
		Assert.assertEquals(expectedFlushs, collector.output.get(TimestampMerger.FLUSH_STREAM_ID));
		Assert.assertEquals(expectedResult, collector.output.get(Utils.DEFAULT_STREAM_ID));
		
		Tuple flushTuple = mock(Tuple.class);
		when(flushTuple.getSourceStreamId()).thenReturn(TimestampMerger.FLUSH_STREAM_ID);
		bolt.execute(flushTuple);
		
		Assert.assertEquals(expectedFlushs.size() + 1, collector.output.get(TimestampMerger.FLUSH_STREAM_ID).size());
		Assert.assertEquals(new Values((Object)null),
			collector.output.get(TimestampMerger.FLUSH_STREAM_ID).get(expectedFlushs.size()));
	}
	
	@Test
	public void testDeclareOutputFields() {
		AccidentDetectionBolt bolt = new AccidentDetectionBolt();
		
		TestDeclarer declarer = new TestDeclarer();
		bolt.declareOutputFields(declarer);
		
		Assert.assertEquals(2, declarer.streamIdBuffer.size());
		Assert.assertEquals(2, declarer.schemaBuffer.size());
		Assert.assertEquals(2, declarer.directBuffer.size());
		
		Assert.assertEquals(TopologyControl.ACCIDENTS_STREAM_ID, declarer.streamIdBuffer.get(0));
		Assert.assertEquals(new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
			TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME).toList(), declarer.schemaBuffer
			.get(0).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(0));
		
		Assert.assertEquals(TimestampMerger.FLUSH_STREAM_ID, declarer.streamIdBuffer.get(1));
		Assert.assertEquals(new Fields("ts").toList(), declarer.schemaBuffer.get(1).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(1));
	}
	
}
