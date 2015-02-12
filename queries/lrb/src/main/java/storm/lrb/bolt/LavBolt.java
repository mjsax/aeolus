package storm.lrb.bolt;

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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;

/**
 * 
 * 
 * this bolt computes the last average vehicle speed (LAV) of the preceeding minute and
 * holds the total count of cars.
 */
public class LavBolt extends BaseRichBolt {
	
	public final static int LAV_XWAY_POS = 0;
	public final static int LAV_DIR_POS = 1;
	public final static int LAV_SEG_POS = 2;
	public final static int LAV_CAR_COUNT_POS = 3;
	public final static int LAV_SPEED_AVG_POS = 4;
	public final static int LAV_MINUTE_POS = 5;
	
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger.getLogger(LavBolt.class);
	protected static final int TOTAL_MINS = 200;
	/**
	 * map xsd to list of avg speeds for every segment and minute
	 * (TODO: change to hold only last five minutes)
	 */
	protected ConcurrentHashMap<Triple<Integer,Integer,Integer>, List<Double>> listOfavgs;

	/**
	 * 
	 */
	protected ConcurrentHashMap<Triple<Integer,Integer,Integer>, List<Integer>> listOfVehicleCounts;
	

	protected Integer lastEmitMinute = 0;
	
	protected Integer currentMinute = 0;
	/**
	 * Holds all lavs and the car cnt
	 */

	private OutputCollector collector;
	private int processed_xway = -1;

	public LavBolt(int xway) {
		processed_xway=xway;
	
		this.listOfavgs = new ConcurrentHashMap<Triple<Integer,Integer,Integer>, List<Double>>();
		this.listOfVehicleCounts = new ConcurrentHashMap<Triple<Integer,Integer,Integer>, List<Integer>>();
	
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		int minuteOfTuple = tuple.getInteger(LAV_MINUTE_POS);
		int xway = tuple.getInteger(LAV_XWAY_POS);
		int seg = tuple.getInteger(LAV_SEG_POS);
		int dir = tuple.getInteger(LAV_DIR_POS);
		int carcnt = tuple.getInteger(LAV_CAR_COUNT_POS);
		double avgs = tuple.getDouble(LAV_SPEED_AVG_POS);

		Triple<Integer,Integer,Integer> segmentKey = new MutableTriple<Integer, Integer, Integer>(xway, seg, dir);
		List<Double> latestAvgSpeeds = listOfavgs.get(segmentKey);
		List<Integer> latestCarCnt = listOfVehicleCounts.get(segmentKey);
	
	
		if (latestAvgSpeeds == null) {
			//initialize the list for the full time so we can use the indices to keep track of time/speed
			latestAvgSpeeds =  new ArrayList(Collections.nCopies(TOTAL_MINS, 0.0));//new ArrayList<Double>();
			listOfavgs.put(segmentKey, latestAvgSpeeds);
		}

		latestAvgSpeeds.add(minuteOfTuple,avgs);
		
		if(latestCarCnt==null){
			latestCarCnt = new ArrayList(Collections.nCopies(TOTAL_MINS, 0));
			listOfVehicleCounts.put(segmentKey, latestCarCnt);
		
		}
	
		latestCarCnt.add(minuteOfTuple, carcnt);
		emitLav(segmentKey, latestCarCnt, latestAvgSpeeds, minuteOfTuple);
		
		collector.ack(tuple);

	}

	/**
	 * loops over the preceeding five items in our list of speedvalues.
	 * and calculates lav
	 * @param speedValues
	 * @param minute
	 * @return lav for the currentminute
	 */
	
	protected double calcLav(List<Double> speedValues, int minute) {
		double sumOfAvgSpeeds = 0;
		int divide = 0;
		for (int i = Math.max(minute-4, 1); i <= minute; i++) {
			sumOfAvgSpeeds += speedValues.get(i);
			if(speedValues.get(i)!=0.0) divide++;
		}
		return sumOfAvgSpeeds / Math.max(divide, 1);
		
	
	}
	
	private void emitLav(Triple<Integer,Integer,Integer> xsd,  List<Integer> vehicleCounts, List<Double> speedValues,
			int minute) {
		
		double speedAverage = calcLav(speedValues, minute);
		int segmentCarCount = vehicleCounts.get(minute);
			
		collector.emit(new Values(
			xsd.getLeft(),
			xsd.getMiddle(),
			xsd.getRight(),
				segmentCarCount, speedAverage, minute+1));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("xway", "dir", "seg", "nov", "lav","minute"));
	}

}
