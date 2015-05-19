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

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.PosReport;
import storm.lrb.model.SegmentStatistics;
import storm.lrb.model.Time;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * This bolt computes the average speed of all cars in a given segment and direction and emits these every minute.
 * 
 * This is the alternative to using AvgsBolt+LavBolt.
 * 
 */
public class SegmentStatsBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(SegmentStatsBolt.class);
	
	private static final int START_MINUTE = 0;
	private static final int AVERAGE_MINS = 5;
	
	/**
	 * contains all statistical information for each segment and minute
	 */
	private final SegmentStatistics segmentStats = new SegmentStatistics();
	
	private OutputCollector collector;
	/*
	 * internal implementation notes: - needs to be long because is unsed for list index access later
	 */
	private int curMinute = 0;
	private String tmpname;
	
	public SegmentStatsBolt() {}
	
	@Override
	public String toString() {
		return "SegmentStats [segmentStats=" + this.segmentStats + ", curMinute=" + this.curMinute + "]";
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.tmpname = context.getThisComponentId() + context.getThisTaskId();
		LOG.info(this.tmpname + " mit diesen sources: " + context.getThisSources().keySet().toString());
		
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		this.countAndAck(tuple);
		
	}
	
	private void emitCurrentWindowCounts() {
		
		long prevMinute = Math.max(this.curMinute - 1, START_MINUTE);
		
		Set<SegmentIdentifier> segmentList = this.segmentStats.getXsdList();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Watching the following segments: %s", segmentList);
		}
		
		// compute the current lav for every segment
		for(SegmentIdentifier xsd : segmentList) {
			int segmentCarCount = 0;
			double speedSum = 0.0;
			int time = Math.max(this.curMinute - AVERAGE_MINS, 1);
			for(; time <= this.curMinute; ++time) {
				if(this.segmentStats.vehicleCount(time, xsd) > 0) {
					segmentCarCount++;
					speedSum += this.segmentStats.speedAverage(time, xsd);
				}
			}
			double speedAverage = 0.0;
			if(segmentCarCount != 0) {
				speedAverage = (speedSum / segmentCarCount);
			}
			this.collector.emit(new Values(xsd.getxWay(), xsd.getSegment(), xsd.getDirection(), segmentCarCount,
				speedAverage, prevMinute));
			
		}
		
	}
	
	private void countAndAck(Tuple tuple) {
		
		PosReport pos = (PosReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
		
		SegmentIdentifier segment = new SegmentIdentifier(pos.getSegmentIdentifier().getxWay(), pos
			.getSegmentIdentifier().getSegment(), pos.getSegmentIdentifier().getDirection());
		
		long newMinute = Time.getMinute(pos.getTime());
		if(newMinute > this.curMinute) {
			this.emitCurrentWindowCounts();
			this.curMinute = (int)Time.getMinute(pos.getTime());
			
		}
		this.segmentStats.addVehicleSpeed(this.curMinute, segment, pos.getVehicleIdentifier(), pos.getCurrentSpeed());
		this.collector.ack(tuple);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
			TopologyControl.DIRECTION_FIELD_NAME, TopologyControl.NUMBER_OF_VEHICLES_FIELD_NAME,
			TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME, TopologyControl.MINUTE_FIELD_NAME));
	}
	
}
