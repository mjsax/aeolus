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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.internal.AvgSpeedTuple;
import de.hub.cs.dbis.lrb.types.internal.LavTuple;
import de.hub.cs.dbis.lrb.types.util.SegmentIdentifier;





/**
 * {@link LatestAverageVelocityBolt} computes the "latest average velocity" (LAV), ie, the average speed over all
 * vehicle within an express way-segment (single direction), over the last five minutes (see Time.getMinute(short)]).
 * The input is expected to be of type {@link AvgSpeedTuple}, to be ordered by timestamp, and must be grouped by
 * {@link SegmentIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link AvgSpeedTuple}<br />
 * <strong>Output schema:</strong> {@link LavTuple} ( (stream: {@link TopologyControl#LAVS_STREAM_ID})
 * 
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class LatestAverageVelocityBolt extends BaseRichBolt {
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOGGER = LoggerFactory.getLogger(LatestAverageVelocityBolt.class);
	
	/** Storm provided output collector. */
	private OutputCollector collector;
	
	/** Internally (re)used object to access individual attributes. */
	private final AvgSpeedTuple inputTuple = new AvgSpeedTuple();
	/** Internally (re)used object. */
	private final SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
	
	/** Holds the (at max) last five average speed value for each segment. */
	private final Map<SegmentIdentifier, List<Double>> averageSpeedsPerSegment = new HashMap<SegmentIdentifier, List<Double>>();
	/** Holds the (at max) last five minute numbers for each segment. */
	private final Map<SegmentIdentifier, List<Short>> minuteNumbersPerSegment = new HashMap<SegmentIdentifier, List<Short>>();
	
	/** The currently processed 'minute number'. */
	private short currentMinute = -1;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		if(input.getSourceStreamId().equals(TimestampMerger.FLUSH_STREAM_ID)) {
			Object ts = input.getValue(0);
			if(ts == null) {
				this.flushBuffer(this.currentMinute + 1);
				this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values((Object)null));
			} else {
				this.checkMinute(((Number)ts).shortValue());
			}
			this.collector.ack(input);
			return;
		}
		
		this.inputTuple.clear();
		this.inputTuple.addAll(input.getValues());
		LOGGER.trace(this.inputTuple.toString());
		
		Short minuteNumber = this.inputTuple.getMinuteNumber();
		short m = minuteNumber.shortValue();
		
		this.checkMinute(m);
		
		this.segmentIdentifier.set(this.inputTuple);
		List<Double> latestAvgSpeeds = this.averageSpeedsPerSegment.get(this.segmentIdentifier);
		List<Short> latestMinuteNumber = this.minuteNumbersPerSegment.get(this.segmentIdentifier);
		
		if(latestAvgSpeeds == null) {
			latestAvgSpeeds = new LinkedList<Double>();
			this.averageSpeedsPerSegment.put(this.segmentIdentifier.copy(), latestAvgSpeeds);
			latestMinuteNumber = new LinkedList<Short>();
			this.minuteNumbersPerSegment.put(this.segmentIdentifier.copy(), latestMinuteNumber);
		}
		latestAvgSpeeds.add(this.inputTuple.getAvgSpeed());
		latestMinuteNumber.add(minuteNumber);
		
		// discard all values that are more than 5 minutes older than current minute
		while(latestAvgSpeeds.size() > 1) {
			if(latestMinuteNumber.get(0).shortValue() < m - 4) {
				latestAvgSpeeds.remove(0);
				latestMinuteNumber.remove(0);
			} else {
				break;
			}
		}
		assert (latestAvgSpeeds.size() <= 5);
		assert (latestMinuteNumber.size() <= 5);
		
		Integer lav = this.computeLavValue(latestAvgSpeeds);
		this.collector.emit(
			TopologyControl.LAVS_STREAM_ID,
			new LavTuple(new Short((short)(m + 1)), this.segmentIdentifier.getXWay(), this.segmentIdentifier
				.getSegment(), this.segmentIdentifier.getDirection(), lav));
		
		this.collector.ack(input);
	}
	
	private void checkMinute(short minute) {
		assert (minute >= this.currentMinute);
		
		if(minute > this.currentMinute) {
			LOGGER.trace("new minute: {}", new Short(minute));
			// each time we step from one minute to another, we need to check the previous time range for "unfinished"
			// open windows; this can happen, if there is not AvgSpeedTuple for a segment in the minute before the
			// current one; we need to truncate all open windows and compute LAV values for each open segment
			this.flushBuffer(minute);
			this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values(new Short(minute)));
			this.currentMinute = minute;
		}
	}
	
	private void flushBuffer(int m) {
		short nextMinute = this.currentMinute;
		// a segment can have multiple consecutive missing AvgSpeedTuple
		// (for example, if no more cars drive on a segment)
		while(nextMinute++ < m) {
			Iterator<Entry<SegmentIdentifier, List<Short>>> it = this.minuteNumbersPerSegment.entrySet().iterator();
			while(it.hasNext()) {
				Entry<SegmentIdentifier, List<Short>> e = it.next();
				SegmentIdentifier sid = e.getKey();
				List<Short> latestMinuteNumber = e.getValue();
				
				if(latestMinuteNumber.get(latestMinuteNumber.size() - 1).shortValue() < nextMinute - 1) {
					List<Double> latestAvgSpeeds = this.averageSpeedsPerSegment.get(sid);
					
					// truncate window if entry is more than 5 minutes older than nextMinute
					// (can be at max one entry)
					if(latestMinuteNumber.get(0).shortValue() < nextMinute - 5) {
						latestAvgSpeeds.remove(0);
						latestMinuteNumber.remove(0);
					}
					
					if(latestAvgSpeeds.size() > 0) {
						Integer lav = this.computeLavValue(latestAvgSpeeds);
						this.collector.emit(TopologyControl.LAVS_STREAM_ID,
							new LavTuple(new Short(nextMinute), sid.getXWay(), sid.getSegment(), sid.getDirection(),
								lav));
					} else {
						// remove empty window completely
						it.remove();
						this.averageSpeedsPerSegment.remove(sid);
					}
				}
			}
		}
	}
	
	private Integer computeLavValue(List<Double> latestAvgSpeeds) {
		double speedSum = 0;
		int valueCount = 0;
		for(Double speed : latestAvgSpeeds) {
			speedSum += speed.doubleValue();
			++valueCount;
		}
		
		return new Integer((int)(speedSum / valueCount));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.LAVS_STREAM_ID, LavTuple.getSchema());
		declarer.declareStream(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
	}
	
}
