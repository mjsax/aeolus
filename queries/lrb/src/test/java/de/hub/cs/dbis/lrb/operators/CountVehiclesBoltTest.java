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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.CountTuple;
import de.hub.cs.dbis.lrb.types.util.SegmentIdentifier;
import de.hub.cs.dbis.lrb.util.Constants;





/**
 * @author mjsax
 */
public class CountVehiclesBoltTest {
	private long seed;
	private Random r;
	
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	@Test
	public void testExecute() {
		final int numberOfSegments = 1 + this.r.nextInt(5);
		final int numberOfMinutes = 5 + this.r.nextInt(5);
		int numberOfTuples = 0;
		
		SegmentIdentifier[] segments = new SegmentIdentifier[numberOfSegments];
		for(int i = 0; i < numberOfSegments; ++i) {
			segments[i] = new SegmentIdentifier(new Integer(this.r.nextInt(2)), new Short(
				(short)this.r.nextInt(Constants.NUMBER_OF_SEGMENT)), new Short((short)this.r.nextInt(1)));
		}
		
		final LinkedList<CountTuple> expectedResult = new LinkedList<CountTuple>();
		
		Tuple tuple = mock(Tuple.class);
		when(tuple.getSourceStreamId()).thenReturn("streamId");
		OngoingStubbing<List<Object>> tupleStub = when(tuple.getValues());
		
		final int startMinute = 1 + this.r.nextInt(5);
		expectedResult.add(new CountTuple(new Short((short)startMinute)));
		for(int m = startMinute; m < startMinute + numberOfMinutes; ++m) {
			final HashMap<SegmentIdentifier, Set<Integer>> counts = new HashMap<SegmentIdentifier, Set<Integer>>();
			
			final int numberOfTuplesMinute = 200 + this.r.nextInt(200);
			numberOfTuples += numberOfTuplesMinute;
			for(int i = 0; i < numberOfTuplesMinute; ++i) {
				
				int time = (m - 1) * 60;
				int lane = this.r.nextInt(5);
				int segIdx = this.r.nextInt(numberOfSegments);
				
				Integer vid = new Integer(this.r.nextInt());
				tupleStub = tupleStub.thenReturn(new PositionReport(new Short((short)time), vid, new Integer(this.r
					.nextInt(Constants.NUMBER_OF_SPEEDS)), segments[segIdx].getXWay(), new Short((short)lane),
					segments[segIdx].getDirection(), segments[segIdx].getSegment(), new Integer(0)));
				
				Set<Integer> cnt = counts.get(segments[segIdx]);
				if(cnt == null) {
					cnt = new HashSet<Integer>();
					cnt.add(vid);
					counts.put(segments[segIdx], cnt);
				} else {
					cnt.add(vid);
				}
			}
			
			final int dummyIndex = expectedResult.size();
			expectedResult.add(new CountTuple(new Short((short)(m + 1))));
			
			for(Entry<SegmentIdentifier, Set<Integer>> e : counts.entrySet()) {
				SegmentIdentifier segId = e.getKey();
				int count = e.getValue().size();
				expectedResult.add(new CountTuple(new Short((short)m), segId.getXWay(), segId.getSegment(), segId
					.getDirection(), new Integer(count)));
			}
			
			if(counts.size() > 0) {
				expectedResult.remove(dummyIndex);
			}
		}
		tupleStub.thenReturn(null);
		
		
		
		CountVehiclesBolt bolt = new CountVehiclesBolt();
		TestOutputCollector collector = new TestOutputCollector();
		bolt.prepare(null, null, new OutputCollector(collector));
		
		for(int i = 0; i < numberOfTuples; ++i) {
			bolt.execute(tuple);
		}
		
		Assert.assertNull(collector.output.get(TimestampMerger.FLUSH_STREAM_ID));
		
		Tuple flushTuple = mock(Tuple.class);
		when(flushTuple.getSourceStreamId()).thenReturn(TimestampMerger.FLUSH_STREAM_ID);
		bolt.execute(flushTuple);
		
		Assert.assertEquals(expectedResult, collector.output.get(TopologyControl.CAR_COUNTS_STREAM_ID));
		
		Assert.assertEquals(1, collector.output.get(TimestampMerger.FLUSH_STREAM_ID).size());
		Assert.assertEquals(new Values(), collector.output.get(TimestampMerger.FLUSH_STREAM_ID).get(0));
	}
	
	@Test
	public void testDeclareOutputFields() {
		AverageSpeedBolt bolt = new AverageSpeedBolt();
		
		TestDeclarer declarer = new TestDeclarer();
		bolt.declareOutputFields(declarer);
		
		Assert.assertEquals(2, declarer.streamIdBuffer.size());
		Assert.assertEquals(2, declarer.schemaBuffer.size());
		Assert.assertEquals(2, declarer.directBuffer.size());
		
		Assert.assertNull(declarer.streamIdBuffer.get(0));
		Assert.assertEquals(new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
			TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.AVERAGE_SPEED_FIELD_NAME).toList(), declarer.schemaBuffer.get(0).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(0));
		
		Assert.assertEquals(TimestampMerger.FLUSH_STREAM_ID, declarer.streamIdBuffer.get(1));
		Assert.assertEquals(new Fields().toList(), declarer.schemaBuffer.get(1).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(1));
	}
	
}
