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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.lrb.types.AvgSpeedTuple;
import de.hub.cs.dbis.lrb.types.AvgVehicleSpeedTuple;
import de.hub.cs.dbis.lrb.types.SegmentIdentifier;
import de.hub.cs.dbis.lrb.util.AvgValue;





/**
 * AverageSpeedBolt computes the average speed over all vehicle within an express way-segment (single direction) every
 * minute. The input is expected to be of type {@link AvgVehicleSpeedTuple} and must be grouped by
 * {@link SegmentIdentifier}. A new average speed computation is trigger each 60 seconds (ie, changing 'minute number'
 * [see Time.getMinute(short)]).<br />
 * <br />
 * <strong>Output schema:</strong> {@link AvgSpeedTuple}
 * 
 * @author mjsax
 */
public class AverageSpeedBolt extends BaseRichBolt {
	private static final long serialVersionUID = -8258719764537430323L;
	
	/** The storm provided output collector. */
	private OutputCollector collector;
	
	/** Internally (re)used object to access individual attributes. */
	private final AvgVehicleSpeedTuple inputTuple = new AvgVehicleSpeedTuple();
	
	/** Maps each segment to its average speed value. */
	private final Map<SegmentIdentifier, AvgValue> avgSpeedsMap = new HashMap<SegmentIdentifier, AvgValue>();
	
	/** The currently processed 'minute number'. */
	private short currentMinute = 1;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		this.inputTuple.clear();
		this.inputTuple.addAll(input.getValues());
		
		short minute = this.inputTuple.getMinute().shortValue();
		int avgVehicleSpeed = this.inputTuple.getAvgSpeed().intValue();
		SegmentIdentifier segment = new SegmentIdentifier(this.inputTuple);
		
		if(minute > this.currentMinute) {
			// emit all values for last minute
			// (because input tuples are ordered by ts (ie, minute number), we can close the last minute safely)
			for(Entry<SegmentIdentifier, AvgValue> entry : this.avgSpeedsMap.entrySet()) {
				SegmentIdentifier segId = entry.getKey();
				
				// Minute-Number, X-Way, Segment, Direction, Avg(speed)
				this.collector.emit(new AvgSpeedTuple(new Short(this.currentMinute), segId.getXWay(), segId
					.getSegment(), segId.getDirection(), entry.getValue().getAverage()));
			}
			
			this.avgSpeedsMap.clear();
			this.currentMinute = minute;
		}
		
		AvgValue segId = this.avgSpeedsMap.get(segment);
		if(segId == null) {
			segId = new AvgValue(avgVehicleSpeed);
			this.avgSpeedsMap.put(segment, segId);
		} else {
			segId.updateAverage(avgVehicleSpeed);
		}
		
		this.collector.ack(input);
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(AvgSpeedTuple.getSchema());
	}
	
}
