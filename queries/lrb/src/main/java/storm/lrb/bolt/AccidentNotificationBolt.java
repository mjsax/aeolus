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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.Accident;
import storm.lrb.model.AccidentImmutable;
import storm.lrb.tools.Constants;
import storm.lrb.tools.Helper;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.lrb.datatypes.PositionReport;
import de.hub.cs.dbis.lrb.util.Time;





/**
 * The Accident notification is for vehicle approching the accident ("upstream of the accident" in LRB terms") to allow
 * them to exit the express way. It receives {@link PosReport}s and {@link Accident}s. The bolt sends back the received
 * {@code PosReport} on {@link Utils#DEFAULT_STREAM_ID} if a previously received accident exists so that the following
 * criteria is matched:
 * 
 * <blockquote>The trigger for an accident notification is a position report q = (Type: 0, Time: t, VID: v, Spd: spd,
 * XWay: x, Seg: s, Pos: p, Lane: l, Dir: d), that identifies a vehicle entering a segment 0 to 4 segments upstream of
 * some accident location, but only if q was emitted no earlier than the minute following the minute when the accident
 * occurred, and no later than the minute the accident is cleared.</blockquote>
 * 
 * LRB only emits accidents if a vehicle crosses a new segment.
 * 
 * @author trillian
 * 
 */
public class AccidentNotificationBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(AccidentNotificationBolt.class);
	
	/**
	 * contains all accidents;
	 */
	private final Map<SegmentIdentifier, AccidentImmutable> allAccidents;
	
	/**
	 * contains all vehicle id's and {@link SegmentIdentifier} of the last {@link PosReport} to allow to skip already
	 * sent notifications (there's only one notification per segment).
	 */
	private final Map<Integer, SegmentIdentifier> allCars;
	
	private OutputCollector collector;
	
	public AccidentNotificationBolt() {
		this.allAccidents = new HashMap<SegmentIdentifier, AccidentImmutable>();
		this.allCars = new HashMap<Integer, SegmentIdentifier>();
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		if(tuple.getSourceStreamId().equals(TopologyControl.POS_REPORTS_STREAM_ID)) {
			
			if(!tuple.contains(TopologyControl.POS_REPORT_FIELD_NAME)) {
				throw new RuntimeException(String.format("tuple without pos report submitted on stream '%s'",
					TopologyControl.POS_REPORTS_STREAM_ID));
			}
			PositionReport pos = (PositionReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
			LOG.debug("AccidentNotification: posReport '%s' received", pos);
			SegmentIdentifier previousSegment = this.allCars.put(pos.getVid(), new SegmentIdentifier(pos));
			// check whether there's an accident on the 4 upcoming segments
			for(short i = 0; i <= 4; i++) {
				Short segmentToCheck = (short)(pos.getSegment() + i);
				LOG.trace("checking segment '{}' for accidents", segmentToCheck);
				SegmentIdentifier accidentSegment = new SegmentIdentifier(pos.getXWay(), segmentToCheck,
					pos.getDirection());
				AccidentImmutable accident = this.allAccidents.get(accidentSegment);
				if(accident != null) {
					this.sendAccidentAlert(pos, previousSegment, accident);
					break; // send a notification for the closest accident only
				}
			}
		} else if(tuple.getSourceStreamId().equals(TopologyControl.ACCIDENT_INFO_STREAM_ID)) {
			if(!tuple.contains(TopologyControl.ACCIDENT_INFO_FIELD_NAME)) {
				throw new RuntimeException(String.format("tuple without accident submitted on stream",
					TopologyControl.ACCIDENT_INFO_STREAM_ID));
			}
			this.updateAccidents(tuple);
		} else {
			throw new RuntimeException(String.format("Errornous stream subscription. Please report a bug at %s",
				Helper.ISSUE_REPORT_URL));
		}
		LOG.debug("tuple: %s", tuple);
		this.collector.ack(tuple);
		
	}
	
	private void updateAccidents(Tuple tuple) {
		PositionReport pos = (PositionReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
		SegmentIdentifier accidentIdentifier = new SegmentIdentifier(pos);
		AccidentImmutable accident = (AccidentImmutable)tuple.getValueByField(TopologyControl.ACCIDENT_INFO_FIELD_NAME);
		
		LOG.debug("received accident info '%s'", accident);
		
		if(accident.isOver()) {
			this.allAccidents.remove(accidentIdentifier);
			LOG.debug("removed accident '%s'", accident);
		} else {
			AccidentImmutable prev = this.allAccidents.put(accidentIdentifier, accident);
			if(prev != null) {
				LOG.debug("accident (prev accident: %s)", prev);
			} else {
				LOG.debug("added new accident");
			}
		}
	}
	
	private void sendAccidentAlert(PositionReport pos, SegmentIdentifier previousSegment, AccidentImmutable accident) {
		// AccidentInfo info = allAccidents.get(accseg);
		
		// only emit notification if vehicle is not involved in accident and accident is still active
		if(accident.getInvolvedCars().contains(pos.getVid())) {
			LOG.debug("no accident notification, because vid is accident vehicle");
			return;
		}
		if(!accident.active(Time.getMinute(pos.getTime()))) {
			LOG.debug("no accident notification because accident is not active anymore");
			// TODO evtl nochmal aufräumen
			return;
		}
		// only emit notification if vehicle crosses new segment and lane is not exit lane
		if(pos.getLane() == Constants.EXIT_LANE) {
			LOG.debug("no accident notification because vehicle is on exit lane");
			return;
		}
		if(!previousSegment.equals(new SegmentIdentifier(pos))) {
			// AccidentImmutable.validatePositionReport(pos);
			this.collector.emit(new Values(pos));
		} else {
			LOG.debug("no accident notification because vehicle exists or was previously informed");
		}
	}
	
	@Override
	public void cleanup() {}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TopologyControl.ACCIDENT_NOTIFICATION_FIELD_NAME));
	}
}
