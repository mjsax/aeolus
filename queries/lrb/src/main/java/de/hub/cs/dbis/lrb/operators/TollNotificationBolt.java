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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.TollNotification;
import de.hub.cs.dbis.lrb.types.internal.AccidentTuple;
import de.hub.cs.dbis.lrb.types.internal.CountTuple;
import de.hub.cs.dbis.lrb.types.internal.LavTuple;
import de.hub.cs.dbis.lrb.types.util.ISegmentIdentifier;
import de.hub.cs.dbis.lrb.types.util.SegmentIdentifier;
import de.hub.cs.dbis.lrb.util.Constants;





/**
 * {@link TollNotificationBolt} calculates the toll for each vehicle and reports it back to the vehicle if a vehicle
 * enters a segment. Furthermore, the toll is assessed to the vehicle if it leaves a segment.<br />
 * <br />
 * The toll depends on the number of cars in the segment (the minute before) the car is driving on and is only charged
 * if the car is not on the exit line, more than 50 cars passed this segment the minute before, the
 * "latest average velocity" is smaller then 40, and no accident occurred in the minute before in the segment and 4
 * downstream segments.<br />
 * <br />
 * {@link TollNotificationBolt} processes four input streams. The first input is expected to be of type
 * {@link PositionReport} and must be grouped by vehicle id. The other inputs are expected to be of type
 * {@link AccidentTuple}, {@link CountTuple}, and {@link LavTuple} and must be broadcasted. All inputs most be ordered
 * by time (ie, timestamp for {@link PositionReport} and minute number for {@link AccidentTuple}, {@link CountTuple},
 * and {@link LavTuple}). It is further assumed, that all {@link AccidentTuple}s and {@link CountTuple}s with a
 * <em>smaller</em> minute number than a {@link PositionReport} tuple as well as all {@link LavTuple}s with the
 * <em>same</em> minute number than a {@link PositionReport} tuple are delivered <em>before</em> those
 * {@link PositionReport}s.<br />
 * <br />
 * This implementation assumes, that {@link PositionReport}s, {@link AccidentTuple}s, {@link CountTuple}s, and
 * {@link LavTuple}s are delivered via streams called {@link TopologyControl#POSITION_REPORTS_STREAM_ID},
 * {@link TopologyControl#ACCIDENTS_STREAM_ID}, {@link TopologyControl#CAR_COUNTS_STREAM_ID}, and
 * {@link TopologyControl#LAVS_STREAM_ID}, respectively.<br />
 * <br />
 * <strong>Expected input:</strong> {@link PositionReport}, {@link AccidentTuple}, {@link CountTuple}, {@link LavTuple}<br />
 * <strong>Output schema:</strong>
 * <ul>
 * <li>{@link TollNotification} (stream: {@link TopologyControl#TOLL_NOTIFICATIONS_STREAM_ID})</li>
 * <li>{@link TollNotification} (stream: {@link TopologyControl#TOLL_ASSESSMENTS_STREAM_ID})</li>
 * </ul>
 * 
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class TollNotificationBolt extends BaseRichBolt {
	private final static long serialVersionUID = 5537727428628598519L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TollNotificationBolt.class);
	
	/** The storm provided output collector. */
	private OutputCollector collector;
	
	/** Buffer for accidents. */
	private Set<ISegmentIdentifier> currentMinuteAccidents = new HashSet<ISegmentIdentifier>();
	/** Buffer for accidents. */
	private Set<ISegmentIdentifier> previousMinuteAccidents = new HashSet<ISegmentIdentifier>();
	/** Buffer for car counts. */
	private Map<ISegmentIdentifier, Integer> currentMinuteCounts = new HashMap<ISegmentIdentifier, Integer>();
	/** Buffer for car counts. */
	private Map<ISegmentIdentifier, Integer> previousMinuteCounts = new HashMap<ISegmentIdentifier, Integer>();
	/** Buffer for LAV values. */
	private Map<ISegmentIdentifier, Integer> currentMinuteLavs = new HashMap<ISegmentIdentifier, Integer>();
	/** Buffer for LAV values. */
	private Map<ISegmentIdentifier, Integer> previousMinuteLavs = new HashMap<ISegmentIdentifier, Integer>();
	/**
	 * Contains all vehicle IDs and segment of the last {@link PositionReport} to allow skipping already sent
	 * notifications (there's only one notification per segment per vehicle).
	 */
	private final Map<Integer, Short> allCars = new HashMap<Integer, Short>();
	/** Contains the last toll notification for each vehicle to assess the toll when the vehicle leaves a segment. */
	private final Map<Integer, TollNotification> lastTollNotification = new HashMap<Integer, TollNotification>();
	
	/** Internally (re)used object to access individual attributes. */
	private final PositionReport inputPositionReport = new PositionReport();
	/** Internally (re)used object to access individual attributes. */
	private final AccidentTuple inputAccidentTuple = new AccidentTuple();
	/** Internally (re)used object to access individual attributes. */
	private final CountTuple inputCountTuple = new CountTuple();
	/** Internally (re)used object to access individual attributes. */
	private final LavTuple inputLavTuple = new LavTuple();
	/** Internally (re)used object. */
	private final SegmentIdentifier segmentToCheck = new SegmentIdentifier();
	
	/** The currently processed 'minute number'. */
	private int currentMinute = -1;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		final String inputStreamId = input.getSourceStreamId();
		if(inputStreamId.equals(TopologyControl.POSITION_REPORTS_STREAM_ID)) {
			this.inputPositionReport.clear();
			this.inputPositionReport.addAll(input.getValues());
			LOGGER.trace(this.inputPositionReport.toString());
			
			this.checkMinute(this.inputPositionReport.getMinuteNumber());
			
			if(this.inputPositionReport.isOnExitLane()) {
				final TollNotification lastNotification = this.lastTollNotification.remove(this.inputPositionReport
					.getVid());
				
				if(lastNotification != null) {
					this.collector.emit(TopologyControl.TOLL_ASSESSMENTS_STREAM_ID, lastNotification);
				}
				
				this.collector.ack(input);
				return;
			}
			
			final Short currentSegment = this.inputPositionReport.getSegment();
			final Integer vid = this.inputPositionReport.getVid();
			final Short previousSegment = this.allCars.put(vid, currentSegment);
			if(previousSegment != null && currentSegment.shortValue() == previousSegment.shortValue()) {
				this.collector.ack(input);
				return;
			}
			
			int toll = 0;
			Integer lav = this.previousMinuteLavs.get(new SegmentIdentifier(this.inputPositionReport));
			
			final int lavValue;
			if(lav != null) {
				lavValue = lav.intValue();
			} else {
				lav = new Integer(0);
				lavValue = 0;
			}
			
			if(lavValue < 40) {
				final Integer count = this.previousMinuteCounts.get(new SegmentIdentifier(this.inputPositionReport));
				int carCount = 0;
				if(count != null) {
					carCount = count.intValue();
				}
				
				if(carCount > 50) {
					// downstream is either larger or smaller of current segment
					final Short direction = this.inputPositionReport.getDirection();
					final short dir = direction.shortValue();
					// EASTBOUND == 0 => diff := 1
					// WESTBOUNT == 1 => diff := -1
					final short diff = (short)-(dir - 1 + ((dir + 1) / 2));
					assert (dir == Constants.EASTBOUND.shortValue() ? diff == 1 : diff == -1);
					
					final Integer xway = this.inputPositionReport.getXWay();
					final short curSeg = currentSegment.shortValue();
					
					this.segmentToCheck.setXWay(xway);
					this.segmentToCheck.setDirection(direction);
					
					int i;
					for(i = 0; i <= 4; ++i) {
						final short nextSegment = (short)(curSeg + (diff * i));
						assert (dir == Constants.EASTBOUND.shortValue() ? nextSegment >= curSeg : nextSegment <= curSeg);
						
						this.segmentToCheck.setSegment(new Short(nextSegment));
						
						if(this.previousMinuteAccidents.contains(this.segmentToCheck)) {
							break;
						}
					}
					
					if(i == 5) { // only true if no accident was found and "break" was not executed
						final int var = carCount - 50;
						toll = 2 * var * var;
					}
				}
			}
			
			// TODO get accurate emit time...
			final TollNotification tollNotification = new TollNotification(this.inputPositionReport.getTime(),
				this.inputPositionReport.getTime(), vid, lav, new Integer(toll));
			
			final TollNotification lastNotification;
			if(toll != 0) {
				lastNotification = this.lastTollNotification.put(vid, tollNotification);
			} else {
				lastNotification = this.lastTollNotification.remove(vid);
			}
			if(lastNotification != null) {
				this.collector.emit(TopologyControl.TOLL_ASSESSMENTS_STREAM_ID, lastNotification);
			}
			
			this.collector.emit(TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, tollNotification);
			
		} else if(inputStreamId.equals(TopologyControl.ACCIDENTS_STREAM_ID)) {
			this.inputAccidentTuple.clear();
			this.inputAccidentTuple.addAll(input.getValues());
			LOGGER.trace(this.inputAccidentTuple.toString());
			
			this.checkMinute(this.inputAccidentTuple.getMinuteNumber().shortValue());
			assert (this.inputAccidentTuple.getMinuteNumber().shortValue() == this.currentMinute);
			
			if(this.inputAccidentTuple.isProgressTuple()) {
				this.collector.ack(input);
				return;
			}
			
			this.currentMinuteAccidents.add(new SegmentIdentifier(this.inputAccidentTuple));
			
		} else if(inputStreamId.equals(TopologyControl.CAR_COUNTS_STREAM_ID)) {
			this.inputCountTuple.clear();
			this.inputCountTuple.addAll(input.getValues());
			LOGGER.trace(this.inputCountTuple.toString());
			
			this.checkMinute(this.inputCountTuple.getMinuteNumber().shortValue());
			assert (this.inputCountTuple.getMinuteNumber().shortValue() == this.currentMinute);
			
			if(this.inputCountTuple.isProgressTuple()) {
				this.collector.ack(input);
				return;
			}
			
			this.currentMinuteCounts.put(new SegmentIdentifier(this.inputCountTuple), this.inputCountTuple.getCount());
			
		} else if(inputStreamId.equals(TopologyControl.LAVS_STREAM_ID)) {
			this.inputLavTuple.clear();
			this.inputLavTuple.addAll(input.getValues());
			LOGGER.trace(this.inputLavTuple.toString());
			
			this.checkMinute((short)(this.inputLavTuple.getMinuteNumber().shortValue() - 1));
			assert (this.inputLavTuple.getMinuteNumber().shortValue() - 1 == this.currentMinute);
			
			this.currentMinuteLavs.put(new SegmentIdentifier(this.inputLavTuple), this.inputLavTuple.getLav());
			
		} else {
			LOGGER.error("Unknown input stream: '" + inputStreamId + "' for tuple " + input);
			throw new RuntimeException("Unknown input stream: '" + inputStreamId + "' for tuple " + input);
		}
		
		this.collector.ack(input);
	}
	
	private void checkMinute(short minute) {
		assert (minute >= this.currentMinute);
		
		if(minute > this.currentMinute) {
			LOGGER.trace("New minute: {}", new Short(minute));
			this.currentMinute = minute;
			this.previousMinuteAccidents = this.currentMinuteAccidents;
			this.currentMinuteAccidents = new HashSet<ISegmentIdentifier>();
			this.previousMinuteCounts = this.currentMinuteCounts;
			this.currentMinuteCounts = new HashMap<ISegmentIdentifier, Integer>();
			this.previousMinuteLavs = this.currentMinuteLavs;
			this.currentMinuteLavs = new HashMap<ISegmentIdentifier, Integer>();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, TollNotification.getSchema());
		declarer.declareStream(TopologyControl.TOLL_ASSESSMENTS_STREAM_ID, TollNotification.getSchema());
	}
	
}
