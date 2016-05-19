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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.internal.StoppedCarTuple;
import de.hub.cs.dbis.lrb.types.util.PositionIdentifier;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * {@link StoppedCarsBolt} registers every stopped vehicle (that are not on the exit lane) and emits "stopped car"
 * reports for further processing. The input is expected to be of type {@link PositionReport}, to be ordered by
 * timestamp, and must be grouped by {@link TopologyControl#VEHICLE_ID_FIELD_NAME}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link StoppedCarTuple}
 * 
 * @author mjsax
 */
public class StoppedCarsBolt extends BaseRichBolt {
	private static final long serialVersionUID = -367873282705758387L;
	private static final Logger LOGGER = LoggerFactory.getLogger(StoppedCarsBolt.class);
	
	/**
	 * Number of consecutive {@link PositionReport}s for a car with same lane and position (grouped by highway and
	 * direction) to consider a car to be "stopped".
	 */
	private static final int STOPPED = 4;
	
	/** The storm provided output collector. */
	private OutputCollector collector;
	
	/** Internally (re)used object to access individual attributes. */
	private final PositionReport inputPositionReport = new PositionReport();
	/** Internally (re)used object. */
	private final PositionIdentifier vehiclePosition = new PositionIdentifier();
	/** Internally (re)used object. */
	private final PositionReport lastPositionReport = new PositionReport();
	/** Internally (re)used object. */
	private final PositionIdentifier lastVehiclePosition = new PositionIdentifier();
	
	/** Holds the last positions for each vehicle (if those positions are equal to each other). */
	private final Map<Integer, List<PositionReport>> lastPositions = new HashMap<Integer, List<PositionReport>>();
	
	/** The currently processed 'minute number'. */
	private int currentMinute = -1;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		if(input.getSourceStreamId().equals(TimestampMerger.FLUSH_STREAM_ID)) {
			if(input.getValue(0) == null) {
				this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values((Object)null));
			}
			this.collector.ack(input);
			return;
		}
		
		this.inputPositionReport.clear();
		this.inputPositionReport.addAll(input.getValues());
		LOGGER.trace(this.inputPositionReport.toString());
		
		Integer vid = this.inputPositionReport.getVid();
		Short time = this.inputPositionReport.getTime();
		int minute = Time.getMinute(time.longValue());
		
		assert (minute >= this.currentMinute);
		
		if(minute > this.currentMinute) {
			LOGGER.trace("New minute: {}", new Integer(minute));
			this.currentMinute = minute;
			this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values(time));
		}
		
		if(this.inputPositionReport.isOnExitLane()) {
			this.lastPositions.remove(vid);
			this.collector.ack(input);
			return;
		}
		
		List<PositionReport> vehiclePositions = this.lastPositions.get(vid);
		if(vehiclePositions == null) {
			vehiclePositions = new LinkedList<PositionReport>();
			vehiclePositions.add(this.inputPositionReport.copy());
			this.lastPositions.put(vid, vehiclePositions);
			
			this.collector.ack(input);
			return;
		}
		
		this.lastPositionReport.clear();
		this.lastPositionReport.addAll(vehiclePositions.get(0));
		
		assert (this.inputPositionReport.getTime().shortValue() == this.lastPositionReport.getTime().shortValue() + 30);
		
		this.vehiclePosition.set(this.inputPositionReport);
		this.lastVehiclePosition.set(this.lastPositionReport);
		
		if(this.vehiclePosition.equals(this.lastVehiclePosition)) {
			vehiclePositions.add(0, this.inputPositionReport.copy());
			
			if(vehiclePositions.size() >= STOPPED) {
				if(vehiclePositions.size() > STOPPED) {
					assert (vehiclePositions.size() == STOPPED + 1);
					vehiclePositions.remove(STOPPED);
				}
				
				this.collector.emit(new StoppedCarTuple(vid, time, this.inputPositionReport.getXWay(),
					this.inputPositionReport.getLane(), this.inputPositionReport.getPosition(),
					this.inputPositionReport.getDirection()));
			}
			
			this.collector.ack(input);
			return;
		}
		
		if(vehiclePositions.size() == STOPPED) { // was "stopped" -- moves again -- send negative VID
			this.collector.emit(new StoppedCarTuple(new Integer(-vid.intValue()), time, this.inputPositionReport
				.getXWay(), this.lastPositionReport.getLane(), this.lastPositionReport.getPosition(),
				this.lastPositionReport.getDirection()));
		}
		vehiclePositions.clear();
		vehiclePositions.add(this.inputPositionReport.copy());
		
		this.collector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(StoppedCarTuple.getSchema());
		declarer.declareStream(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
	}
	
}
