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

import storm.lrb.model.AvgVehicleSpeeds;
import storm.lrb.model.PosReport;

import storm.lrb.model.Time;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This bolt computes the average speed of a vehicle in a given segment and
 * direction and emits these every minute
 * 
 */
public class AvgsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger.getLogger(AvgsBolt.class);

	

	/**
	 * contains the time of the last reports of each segment
	 * (xsd, timeinsec)
	 */
	protected ConcurrentHashMap<SegmentIdentifier, Integer> timeOfLastReportsMap;
	/**
	 * contains all avgs of each vehicle driving in the given segment
	 * for the current minute. gets resetted every minute 
	 * (xsd -> Map(minute ->avgsvehiclespeeds))
	 * 
	 */
	protected ConcurrentHashMap<SegmentIdentifier, AvgVehicleSpeeds> avgSpeedsMap; // xsd
																		// =>
																		// List<avg
																		// vehicles
																		// speeds>

	private volatile Integer lastEmitMinute = 0;
	private OutputCollector collector;
	private int processed_xway = -1;

	public AvgsBolt(int xway) {
		timeOfLastReportsMap = new ConcurrentHashMap<SegmentIdentifier, Integer>();
		avgSpeedsMap = new ConcurrentHashMap<SegmentIdentifier, AvgVehicleSpeeds>();
		processed_xway = xway;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple tuple) {

		PosReport pos = (PosReport) tuple.getValueByField("PosReport");

		
		SegmentIdentifier accidentIdentifier = new SegmentIdentifier(
			pos.getSegmentIdentifier().getxWay(), pos.getSegmentIdentifier().getSegment(), pos.getSegmentIdentifier().getDirection());

		int curminute = Time.getMinute(pos.getTime());

		
		//synchronized (lastEmitMinute) {
		
		// if a new minute starts emit all previous accumulated avgs
		if (lastEmitMinute < curminute) {
			lastEmitMinute = curminute;
			emitAllAndRemove(curminute - 1);
			
		}
		//}

		Integer timeOfLastReports = timeOfLastReportsMap.get(accidentIdentifier);
		if (timeOfLastReports == null) {
			timeOfLastReports = curminute;
			timeOfLastReportsMap.put(accidentIdentifier, timeOfLastReports);
		}

		AvgVehicleSpeeds lastSpeeds = avgSpeedsMap.get(accidentIdentifier);
		// synchronized(lastSpeeds){
		if (lastSpeeds == null) {
			lastSpeeds = new AvgVehicleSpeeds();
			avgSpeedsMap.put(accidentIdentifier, lastSpeeds);
		}

		timeOfLastReportsMap.put(accidentIdentifier, curminute);
		lastSpeeds.addVehicleSpeed(pos.getVehicleIdentifier(), pos.getCurrentSpeed());
		// }

		collector.ack(tuple);
	}

	private void emitAllAndRemove(int minute) {

		Set<SegmentIdentifier> segmentList = avgSpeedsMap.keySet();

		for (SegmentIdentifier xsd : segmentList) {
			AvgVehicleSpeeds lastSpeeds = avgSpeedsMap.get(xsd);
			if (lastSpeeds != null) {
				collector.emit(new Values(processed_xway, xsd, lastSpeeds.vehicleCount(), lastSpeeds.speedAverage(), minute));
				avgSpeedsMap.replace(xsd, new AvgVehicleSpeeds());
			}

		}

	}

	private void emitAndRemove(SegmentIdentifier xsd, int minute) {

		AvgVehicleSpeeds lastSpeeds = avgSpeedsMap.get(xsd);
		if (lastSpeeds != null) {
			synchronized (lastSpeeds) {
				collector.emit(new Values(processed_xway, xsd, lastSpeeds.vehicleCount(), lastSpeeds.speedAverage(), minute));
				avgSpeedsMap.replace(xsd, new AvgVehicleSpeeds());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("xway", "xsd", "carcnt", "avgs", "minute"));
	}

}
