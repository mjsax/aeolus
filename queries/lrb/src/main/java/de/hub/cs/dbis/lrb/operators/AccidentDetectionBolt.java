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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;
import de.hub.cs.dbis.lrb.types.internal.StoppedCarTuple;
import de.hub.cs.dbis.lrb.types.util.PositionIdentifier;
import de.hub.cs.dbis.lrb.util.Constants;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * {@link AccidentDetectionBolt} processed stopped vehicle reports and emits accident information for further
 * processing. The input is expected to be of type {@link StoppedCarTuple}, to be ordered by timestamp, and must be
 * grouped by {@link PositionIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link StoppedCarTuple}<br />
 * <strong>Output schema:</strong> {@link AccidentTuple} (stream: {@link TopologyControl#ACCIDENTS_STREAM_ID})
 * 
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class AccidentDetectionBolt extends BaseRichBolt {
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOGGER = LoggerFactory.getLogger(AccidentDetectionBolt.class);
	
	/** The storm provided output collector. */
	private OutputCollector collector;
	
	/** Internally (re)used object to access individual attributes. */
	private final StoppedCarTuple inputStoppedCarTuple = new StoppedCarTuple();
	/** Internally (re)used object. */
	private final PositionIdentifier stoppedVehiclePosition = new PositionIdentifier();
	
	/** Hold all vehicles that have <em>stopped</em> within a segment. */
	private final Map<PositionIdentifier, Set<Integer>> stoppedCarsPerPosition = new HashMap<PositionIdentifier, Set<Integer>>();
	
	/** The currently processed 'minute number'. */
	private short currentMinute = -1;
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		if(input.getSourceStreamId().equals(TimestampMerger.FLUSH_STREAM_ID)) {
			Object ts = input.getValue(0);
			if(ts == null) {
				this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values((Object)null));
			} else {
				this.checkMinute(Time.getMinute(((Number)ts).longValue()));
			}
			this.collector.ack(input);
			return;
		}
		
		this.inputStoppedCarTuple.clear();
		this.inputStoppedCarTuple.addAll(input.getValues());
		LOGGER.trace(this.inputStoppedCarTuple.toString());
		
		this.checkMinute(this.inputStoppedCarTuple.getMinuteNumber());
		assert (this.inputStoppedCarTuple.getLane().shortValue() != Constants.EXIT_LANE);
		
		Integer vid = this.inputStoppedCarTuple.getVid();
		// negative VID implies "move" after "stop"
		int v = vid.intValue();
		boolean isStopReport = v > 0;
		
		this.stoppedVehiclePosition.set(this.inputStoppedCarTuple);
		
		Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.stoppedVehiclePosition);
		if(isStopReport) {
			if(stoppedCars == null) {
				stoppedCars = new HashSet<Integer>();
				stoppedCars.add(vid);
				this.stoppedCarsPerPosition.put(this.stoppedVehiclePosition.copy(), stoppedCars);
			} else {
				stoppedCars.add(vid);
				
				if(stoppedCars.size() > 1) {
					this.collector.emit(TopologyControl.ACCIDENTS_STREAM_ID, new AccidentTuple(
						this.inputStoppedCarTuple.getTime(), this.inputStoppedCarTuple.getXWay(),
						this.inputStoppedCarTuple.getSegment(), this.inputStoppedCarTuple.getDirection()));
				}
			}
		} else {
			stoppedCars.remove(new Integer(-v));
			if(stoppedCars.isEmpty()) {
				this.stoppedCarsPerPosition.remove(this.inputStoppedCarTuple);
			}
		}
		
		this.collector.ack(input);
	}
	
	private void checkMinute(short minute) {
		assert (minute >= this.currentMinute);
		
		if(minute > this.currentMinute) {
			LOGGER.trace("New minute: {}", new Short(minute));
			this.currentMinute = minute;
			this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values(new Short((short)((minute * 60) - 61))));
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.ACCIDENTS_STREAM_ID, AccidentTuple.getSchema());
		declarer.declareStream(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
	}
	
}
