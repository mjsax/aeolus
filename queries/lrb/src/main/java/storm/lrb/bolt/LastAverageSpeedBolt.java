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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * 
 * 
 * this bolt computes the last average vehicle speed (LAV) of the preceeding minute and holds the total count of cars.
 */
public class LastAverageSpeedBolt extends BaseRichBolt {
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(LastAverageSpeedBolt.class);
	protected static final int TOTAL_MINS = 200;
	/**
	 * map xsd to list of avg speeds for every segment and minute (TODO: change to hold only last five minutes)
	 */
	private final Map<Triple<Integer, SegmentIdentifier, Integer>, List<Double>> listOfavgs;
	
	private final Map<Triple<Integer, SegmentIdentifier, Integer>, List<Integer>> listOfVehicleCounts;
	
	private OutputCollector collector;
	
	public LastAverageSpeedBolt() {
		this.listOfavgs = new HashMap<Triple<Integer, SegmentIdentifier, Integer>, List<Double>>();
		this.listOfVehicleCounts = new HashMap<Triple<Integer, SegmentIdentifier, Integer>, List<Integer>>();
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		int minuteOfTuple = tuple.getIntegerByField(TopologyControl.MINUTE_FIELD_NAME);
		int xway = tuple.getIntegerByField(TopologyControl.XWAY_FIELD_NAME);
		SegmentIdentifier segmentIdentifier = (SegmentIdentifier)tuple
			.getValueByField(TopologyControl.SEGMENT_FIELD_NAME);
		int direction = tuple.getIntegerByField(TopologyControl.DIRECTION_FIELD_NAME);
		int carcnt = tuple.getIntegerByField(TopologyControl.CAR_COUNT_FIELD_NAME);
		double avgs = tuple.getDoubleByField(TopologyControl.AVERAGE_SPEED_FIELD_NAME);
		
		Triple<Integer, SegmentIdentifier, Integer> segmentKey = new MutableTriple<Integer, SegmentIdentifier, Integer>(
			xway, segmentIdentifier, direction);
		List<Double> latestAvgSpeeds = this.listOfavgs.get(segmentKey);
		List<Integer> latestCarCnt = this.listOfVehicleCounts.get(segmentKey);
		
		if(latestAvgSpeeds == null) {
			// initialize the list for the full time so we can use the indices to keep track of time/speed
			latestAvgSpeeds = new ArrayList<Double>(Collections.nCopies(TOTAL_MINS, 0.0));// new ArrayList<Double>();
			this.listOfavgs.put(segmentKey, latestAvgSpeeds);
		}
		
		latestAvgSpeeds.add(minuteOfTuple, avgs);
		
		if(latestCarCnt == null) {
			latestCarCnt = new ArrayList<Integer>(Collections.nCopies(TOTAL_MINS, 0));
			this.listOfVehicleCounts.put(segmentKey, latestCarCnt);
			
		}
		
		latestCarCnt.add(minuteOfTuple, carcnt);
		this.emitLav(xway, direction, segmentIdentifier, latestCarCnt, latestAvgSpeeds, minuteOfTuple);
		
		this.collector.ack(tuple);
		
	}
	
	/**
	 * loops over the preceeding five items in our list of speedvalues. and calculates lav
	 * 
	 * @param speedValues
	 * @param minute
	 * @return lav for the currentminute of {@code 0} if {@code minute} is {@code <=0}
	 */
	protected double calcLav(List<Double> speedValues, int minute) {
		if(speedValues == null || speedValues.isEmpty()) {
			throw new IllegalArgumentException("speedValues mustn't be null or empty");
		}
		if(minute >= speedValues.size()) {
			throw new IllegalArgumentException(String.format("minute %d must be less than items in speedValues %d",
				minute, speedValues.size()));
		}
		double sumOfAvgSpeeds = 0;
		int divide = 0;
		int indexMax = Math.max(minute - 4, 1);
		for(int i = indexMax; i <= minute; i++) {
			// start at 1 because only the 5 preceeding items count
			sumOfAvgSpeeds += speedValues.get(i);
			if(speedValues.get(i) != 0.0) {
				divide++;
			}
		}
		return sumOfAvgSpeeds / Math.max(divide, 1);
		
	}
	
	private void emitLav(int xWay, int direction, SegmentIdentifier segmentIdentifier, List<Integer> vehicleCounts, List<Double> speedValues, int minute) {
		
		double speedAverage = this.calcLav(speedValues, minute);
		int segmentCarCount = vehicleCounts.get(minute);
		
		this.collector.emit(new Values(xWay, direction, segmentIdentifier, segmentCarCount, speedAverage, minute + 1));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, new Fields(
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
			TopologyControl.NUMBER_OF_VEHICLES_FIELD_NAME, TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME,
			TopologyControl.MINUTE_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME));
	}
	
}
