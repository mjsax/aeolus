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
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;
import de.hub.cs.dbis.lrb.types.internal.StoppedCarTuple;
import de.hub.cs.dbis.lrb.util.Constants;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
public class AccidentDetectionBoltTest {
	// @formatter:off
	// segment size = 5280 with seg_0 = [0..5179]
	@SuppressWarnings("boxing") private static Integer[][][] timeAndPositions = new Integer[][][] {
		// vehicle 1
		new Integer[][] {new Integer[] {0, 151, 50102, 1},
						 new Integer[] {0, 181, 50102, -1}, // move
						 new Integer[] {0, 301, 50103, 1},
						 new Integer[] {0, 331, 50103, 1},
						 new Integer[] {0, 361, 50103, -1}, // move
						 new Integer[] {0, 481, 50105, 1}, // accident m=9
		},
		// vehicle 2
		new Integer[][] {new Integer[] {0, 185, 50102, 1},
						 new Integer[] {0, 215, 50102, -1}, // move
						 new Integer[] {0, 425, 50105, 1}, // accident m=8
						 new Integer[] {0, 455, 50105, 1}, // accident m=8
						 new Integer[] {0, 485, 50105, -1}, // move
		},
		// vehicle 3
		new Integer[][] {new Integer[] {0, 130, 50105, 1},
						 new Integer[] {0, 160, 50105, 1},
						 new Integer[] {0, 190, 50105, 1},
						 new Integer[] {0, 220, 50105, 1},
						 new Integer[] {0, 250, 50105, 1},
						 new Integer[] {0, 280, 50105, 1},
						 new Integer[] {0, 310, 50105, 1},
						 new Integer[] {0, 340, 50105, 1},
						 new Integer[] {0, 370, 50105, 1},
						 new Integer[] {0, 400, 50105, 1},
						 new Integer[] {0, 430, 50105, 1}, // accident m=8
						 new Integer[] {0, 460, 50105, 1}, // accident m=8
						 new Integer[] {0, 490, 50105, -1}, // move
		}
	};
	// @formatter:on
	
	@SuppressWarnings("boxing")
	@Test
	public void testExecute() {
		Tuple tuple = mock(Tuple.class);
		when(tuple.getSourceStreamId()).thenReturn("streamId");
		OngoingStubbing<List<Object>> tupleValueStub = when(tuple.getValues());
		
		int[] m = new int[timeAndPositions.length];
		int tupleCount = 0;
		while(true) {
			int vid = -1;
			int minTs = Integer.MAX_VALUE;
			
			for(int i = 0; i < timeAndPositions.length; ++i) {
				if(m[i] < timeAndPositions[i].length) {
					if(timeAndPositions[i][m[i]][1] < minTs) {
						minTs = timeAndPositions[i][m[i]][1];
						vid = i;
					}
				}
			}
			
			if(minTs == Integer.MAX_VALUE) {
				break;
			}
			
			StoppedCarTuple t = new StoppedCarTuple((vid + 1) * timeAndPositions[vid][m[vid]][3],
				timeAndPositions[vid][m[vid]][1].shortValue(), 0, timeAndPositions[vid][m[vid]][0].shortValue(),
				timeAndPositions[vid][m[vid]][2], Constants.EASTBOUND);
			tupleValueStub = tupleValueStub.thenReturn(t);
			++tupleCount;
			
			++m[vid];
		}
		tupleValueStub.thenReturn(null);
		
		AccidentDetectionBolt bolt = new AccidentDetectionBolt();
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		for(int i = 0; i < tupleCount; ++i) {
			bolt.execute(tuple);
		}
		
		Tuple flushTuple = mock(Tuple.class);
		when(flushTuple.getSourceStreamId()).thenReturn(TimestampMerger.FLUSH_STREAM_ID);
		bolt.execute(flushTuple);
		
		List<Values> expectedFlushes = new ArrayList<Values>();
		expectedFlushes.add(new Values((short)119));
		expectedFlushes.add(new Values((short)179));
		expectedFlushes.add(new Values((short)239));
		expectedFlushes.add(new Values((short)299));
		expectedFlushes.add(new Values((short)359));
		expectedFlushes.add(new Values((short)419));
		expectedFlushes.add(new Values((short)479));
		expectedFlushes.add(new Values((Object)null));
		
		List<Values> expectedResult = new ArrayList<Values>();
		expectedResult.add(new AccidentTuple((short)425, 0, (short)9, (short)0));
		expectedResult.add(new AccidentTuple((short)430, 0, (short)9, (short)0));
		expectedResult.add(new AccidentTuple((short)455, 0, (short)9, (short)0));
		expectedResult.add(new AccidentTuple((short)460, 0, (short)9, (short)0));
		expectedResult.add(new AccidentTuple((short)481, 0, (short)9, (short)0));
		
		Assert.assertEquals(2, collector.output.size());
		Assert.assertEquals(expectedResult, collector.output.get(TopologyControl.ACCIDENTS_STREAM_ID));
		Assert.assertEquals(expectedFlushes, collector.output.get(TimestampMerger.FLUSH_STREAM_ID));
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
		Assert.assertEquals(new Fields(TopologyControl.TIMESTAMP_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
			TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME).toList(), declarer.schemaBuffer
			.get(0).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(0));
		
		Assert.assertEquals(TimestampMerger.FLUSH_STREAM_ID, declarer.streamIdBuffer.get(1));
		Assert.assertEquals(new Fields("ts").toList(), declarer.schemaBuffer.get(1).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(1));
	}
	
}
