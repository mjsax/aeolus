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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import storm.lrb.TopologyControl;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.lrb.testutils.AbstractLRBTest;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.types.AvgSpeedTuple;
import de.hub.cs.dbis.lrb.types.LavTuple;
import de.hub.cs.dbis.lrb.types.SegmentIdentifier;
import de.hub.cs.dbis.lrb.util.Constants;





/**
 * @author richter
 * @author mjsax
 */
public class LastAverageVelocityBoltTest extends AbstractLRBTest {
	
	@Test
	public void testExecute() {
		LatestAverageVelocityBolt instance = new LatestAverageVelocityBolt();
		
		TestOutputCollector collector = new TestOutputCollector();
		instance.prepare(null, null, new OutputCollector(collector));
		
		final short numberOfMinutes = 10;
		final int numberOfHighways = 1 + this.r.nextInt(3);
		final int numberOfSegments = 1 + this.r.nextInt(5);
		
		final Map<SegmentIdentifier, List<Integer>> allSpeedsPerSegments = new HashMap<SegmentIdentifier, List<Integer>>();
		final List<SegmentIdentifier> allSegments = new LinkedList<SegmentIdentifier>();
		final List<SegmentIdentifier> skippedSegments = new LinkedList<SegmentIdentifier>();
		
		
		Tuple tuple = mock(Tuple.class);
		OngoingStubbing<List<Object>> tupleStub = when(tuple.getValues());
		
		for(short minute = 1; minute <= numberOfMinutes; ++minute) {
			List<AvgSpeedTuple> input = new LinkedList<AvgSpeedTuple>();
			
			for(int xway = 1; xway <= numberOfHighways; ++xway) {
				for(short segment = 1; segment <= numberOfSegments; ++segment) {
					// randomly skip some AvgSpeedTuple
					if(this.r.nextDouble() < 0.5 / numberOfSegments) {
						input.add(null);
						skippedSegments.add(new SegmentIdentifier(new Integer(xway), new Short(segment),
							Constants.EASTBOUND));
						continue;
					}
					
					int avgSpeed = 1 + this.r.nextInt(Constants.MAX_SPEED);
					input.add(new AvgSpeedTuple(new Short(minute), new Integer(xway), new Short(segment),
						Constants.EASTBOUND, new Integer(avgSpeed)));
				}
			}
			
			Collections.shuffle(input, this.r);
			for(AvgSpeedTuple t : input) {
				SegmentIdentifier sid;
				if(t != null) {
					sid = new SegmentIdentifier(t);
				} else {
					sid = skippedSegments.remove(0);
				}
				allSegments.add(sid);
				
				List<Integer> speeds = allSpeedsPerSegments.get(sid);
				if(speeds == null) {
					speeds = new LinkedList<Integer>();
					allSpeedsPerSegments.put(sid, speeds);
				}
				
				if(t != null) {
					speeds.add(t.getAvgSpeed());
					tupleStub = tupleStub.thenReturn(t);
				} else {
					speeds.add(null);
				}
			}
		}
		
		int executeCounter = 0;
		int lastMinute = 0;
		for(short minute = 1; minute <= numberOfMinutes; ++minute) {
			boolean firstTupleOfMinute = true;
			for(int i = 0; i < numberOfHighways * numberOfSegments; ++i) {
				SegmentIdentifier sid = allSegments.remove(0);
				List<Integer> speeds = allSpeedsPerSegments.get(sid);
				if(speeds.get(minute - 1) == null) {
					continue;
				}
				
				instance.execute(tuple);
				
				List<LavTuple> expectedResult = new LinkedList<LavTuple>();
				if(firstTupleOfMinute && minute > 1) { // process open (incomplete windows)
					for(int m = minute - 5; m < minute - 1; ++m) {
						if(m < 0 || m <= lastMinute) {
							continue;
						}
						lastMinute = m;
						for(Entry<SegmentIdentifier, List<Integer>> e : allSpeedsPerSegments.entrySet()) {
							List<Integer> speedList = e.getValue();
							if(speedList.get(m) == null) {
								this.addResult(speedList, (short)(m + 1), expectedResult, e.getKey());
							}
						}
					}
				}
				
				this.addResult(speeds, minute, expectedResult, sid);
				
				assertEquals(1, collector.output.size());
				List<List<Object>> result = collector.output.get(Utils.DEFAULT_STREAM_ID);
				assertLavTupleInputEquals(expectedResult, result, ++executeCounter, collector);
				
				firstTupleOfMinute = false;
			}
		}
	}
	
	private void addResult(List<Integer> speeds, short minute, List<LavTuple> expectedResult, SegmentIdentifier sid) {
		int sum = 0;
		int cnt = 0;
		List<Integer> window = speeds.subList(minute > 5 ? minute - 5 : 0, minute);
		for(Integer avgs : window) {
			if(avgs == null) {
				continue;
			}
			sum += avgs.intValue();
			++cnt;
		}
		if(cnt == 0) {
			return;
		}
		expectedResult.add(new LavTuple(new Short((short)(minute + 1)), sid.getXWay(), sid.getSegment(), sid
			.getDirection(), new Integer(sum / cnt)));
	}
	
	@Test
	public void testDeclareOutputFields() {
		LatestAverageVelocityBolt bolt = new LatestAverageVelocityBolt();
		
		TestDeclarer declarer = new TestDeclarer();
		bolt.declareOutputFields(declarer);
		
		Assert.assertEquals(1, declarer.streamIdBuffer.size());
		Assert.assertEquals(1, declarer.schemaBuffer.size());
		Assert.assertEquals(1, declarer.directBuffer.size());
		
		Assert.assertNull(declarer.streamIdBuffer.get(0));
		Assert.assertEquals(new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
			TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME).toList(), declarer.schemaBuffer.get(0).toList());
		Assert.assertEquals(new Boolean(false), declarer.directBuffer.get(0));
	}
	
}
