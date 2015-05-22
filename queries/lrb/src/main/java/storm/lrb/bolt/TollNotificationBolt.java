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
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.NovLav;
import storm.lrb.model.PosReport;
import storm.lrb.model.Time;
import storm.lrb.model.VehicleInfo;
import storm.lrb.tools.StopWatch;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * This bolt calculates the actual toll for each vehicle and reports it back to the data driver. The toll depends on the
 * congestion and accident status of the segment the vehicle is driving on. It can process streams containing position
 * reports, accident information and novLavs.
 */
public class TollNotificationBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(TollNotificationBolt.class);

	protected static final int MAX_SPEED_FOR_TOLL = 40;
	protected static final int MIN_CARS_FOR_TOLL = 50;
	private final static int DRIVE_EASY = 0;
	public static final Fields FIELDS_OUTGOING_TOLL_NOTIFICATION = new Fields(
		TopologyControl.TOLL_NOTIFICATION_FIELD_NAME);
	public static final Fields FIELDS_OUTGOING_TOLL_ASSESSMENT = new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME,
		TopologyControl.XWAY_FIELD_NAME, TopologyControl.TOLL_ASSESSED_FIELD_NAME,
		TopologyControl.POS_REPORT_FIELD_NAME);

	/**
	 * Holds all lavs (avgs of preceeding minute) and novs (number of vehicles) of the preceeding minute in a segment
	 * segment -> NovLav
	 */
	private final Map<SegmentIdentifier, NovLav> allNovLavs;

	/**
	 * holds vehicle information of all vehicles driving (vid -> vehicleinfo)
	 */
	private final Map<Integer, VehicleInfo> allVehicles;

	/**
	 * holds all current accidents (xsd -> Accidentinformation)
	 */
	private final Map<SegmentIdentifier, AccidentImmutable> allAccidents;

	// protected BlockingQueue<Tuple> waitingList;
	private OutputCollector collector;

	private String tmpname;
	private final StopWatch timer;

	public TollNotificationBolt(StopWatch timer) {
		this.allNovLavs = new HashMap<SegmentIdentifier, NovLav>();
		this.allVehicles = new HashMap<Integer, VehicleInfo>();
		this.allAccidents = new HashMap<SegmentIdentifier, AccidentImmutable>();
		// waitingList = new LinkedBlockingQueue<Tuple>();
		this.timer = timer;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.tmpname = context.getThisComponentId() + context.getThisTaskId();
		LOG.info("Component '%s' subscribed to: %s", this.tmpname, context.getThisSources().keySet().toString());
	}

	@Override
	public void execute(Tuple tuple) {
		if(tuple.contains(TopologyControl.POS_REPORT_FIELD_NAME)) {
			this.calcTollAndEmit(tuple);

		} else if(tuple.contains(TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME)) {
			this.updateNovLavs(tuple);

		} else if(tuple.contains(TopologyControl.ACCIDENT_INFO_FIELD_NAME)) {
			this.updateAccidents(tuple);
		}

		this.collector.ack(tuple);
	}

	private void updateNovLavs(Tuple tuple) {
		int xWay = tuple.getIntegerByField(TopologyControl.XWAY_FIELD_NAME);
		int seg = tuple.getIntegerByField(TopologyControl.SEGMENT_FIELD_NAME);
		int dir = tuple.getIntegerByField(TopologyControl.DIRECTION_FIELD_NAME);
		SegmentIdentifier segmentIdentifier = new SegmentIdentifier(xWay, seg, dir);
		int min = tuple.getIntegerByField(TopologyControl.MINUTE_FIELD_NAME);

		NovLav novlav = new NovLav(tuple.getIntegerByField(TopologyControl.NUMBER_OF_VEHICLES_FIELD_NAME),
			tuple.getDoubleByField(TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME),
			tuple.getIntegerByField(TopologyControl.MINUTE_FIELD_NAME));

		this.allNovLavs.put(segmentIdentifier, novlav);
		// currentNovLavMinutes.put(tuple.getStringByField("xsd"), min);
		if(LOG.isDebugEnabled()) {
			LOG.debug("updated novlavs for xway %d, segment %d, direction %d; novlav: %s", xWay, seg, dir, novlav);
		}
	}

	private void updateAccidents(Tuple tuple) {
		int xway = tuple.getIntegerByField(TopologyControl.XWAY_FIELD_NAME);
		int seg = tuple.getIntegerByField(TopologyControl.SEGMENT_FIELD_NAME);
		int dir = tuple.getIntegerByField(TopologyControl.DIRECTION_FIELD_NAME);
		SegmentIdentifier xsd = new SegmentIdentifier(xway, seg, dir);

		AccidentImmutable info = (AccidentImmutable)tuple.getValueByField(TopologyControl.ACCIDENT_INFO_FIELD_NAME);
		if(LOG.isDebugEnabled()) {
			LOG.debug("received accident info: %s", info);
		}

		if(info.isOver()) {
			this.allAccidents.remove(xsd);
			if(LOG.isDebugEnabled()) {
				LOG.debug("TOLLN:: removed accident: " + info);
			}
		} else {
			AccidentImmutable prev = this.allAccidents.put(xsd, info);
			if(prev != null) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("TOLLN: accident (prev: " + prev + ")");
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug("TOLLN:: added new accident");
					}
				}
			}
		}
	}

	void calcTollAndEmit(Tuple tuple) {

		PosReport pos = (PosReport)tuple.getValueByField("PosReport");
		SegmentIdentifier segmentTriple = new SegmentIdentifier(pos.getSegmentIdentifier().getxWay(), pos
			.getSegmentIdentifier().getSegment(), pos.getSegmentIdentifier().getDirection());

		if(this.assessTollAndCheckIfTollNotificationRequired(pos)) {

			int toll = this.calcToll(segmentTriple, Time.getMinute(pos.getTime()));
			double lav = 0.0;
			int nov = 0;
			if(this.allNovLavs.containsKey(segmentTriple)) {
				lav = this.allNovLavs.get(segmentTriple).getLav();
				nov = this.allNovLavs.get(segmentTriple).getNov();
			}

			String notification = this.allVehicles.get(pos.getVehicleIdentifier()).getTollNotification(lav, toll, nov);
			if(LOG.isDebugEnabled() && toll > 0) {
				LOG.debug("TOLLN: actually calculated toll (=" + toll + ") for vid=" + pos.getVehicleIdentifier()
					+ " at " + pos.getTime() + " sec");
			}

			if(notification.isEmpty()) {
				LOG.info("TOLLN: duplicate toll:" + this.allVehicles.get(pos.getVehicleIdentifier()).toString());

			} else {
				this.collector.emit(TopologyControl.TOLL_NOTIFICATION_STREAM_ID, new Values(notification));
			}

		}
	}

	/**
	 * Calculate the toll amount according to the current congestion of the segment
	 *
	 * @param position
	 * @param minute
	 * @return toll amount to charge the vehicle with
	 */
	protected int calcToll(SegmentIdentifier position, long minute) {
		int toll = DRIVE_EASY;
		int nov = 0;
		if(this.allNovLavs.containsKey(position)) {
			int novLavMin = this.allNovLavs.get(position).getMinute();
			if(novLavMin == minute || novLavMin + 1 == minute) {
				nov = this.allNovLavs.get(position).getNov();
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("The novLav is not available or up to date: " + this.allNovLavs.get(position)
						+ "current minute" + minute);
				}
			}
		}

		if(this.tollConditionSatisfied(position, minute)) {
			toll = (int)(2 * Math.pow(nov - 50, 2));
		}

		return toll;
	}

	/**
	 * Assess toll of previous notification and check if tollnotification is required and in the course update the
	 * vehilces information
	 *
	 * @param posReport
	 * @return true if tollnotification is required
	 */
	protected boolean assessTollAndCheckIfTollNotificationRequired(PosReport posReport) {
		boolean segmentChanged;
		if(!this.allVehicles.containsKey(posReport.getVehicleIdentifier())) {
			segmentChanged = true;
			this.allVehicles.put(posReport.getVehicleIdentifier(), new VehicleInfo(posReport));
		} else {
			VehicleInfo vehicle = this.allVehicles.get(posReport.getVehicleIdentifier());
			SegmentIdentifier oldPosition = vehicle.getSegmentIdentifier();
			SegmentIdentifier newPosition = posReport.getSegmentIdentifier();
			segmentChanged = !oldPosition.equals(newPosition);
			if(LOG.isDebugEnabled()) {
				LOG.debug("assess toll:" + vehicle);
			}
			// assess previous toll by emitting toll info to be processed by accountbaancebolt
			this.collector.emit(TopologyControl.TOLL_ASSESSMENT_STREAM_ID, new Values(posReport.getVehicleIdentifier(),
				vehicle.getXway(), vehicle.getToll(), posReport));

			vehicle.updateInfo(posReport);
		}

		return segmentChanged && !posReport.isOnExitLane();
	}

	/**
	 * Check if the condition for charging toll, which depends on the minute and segment of the vehicle, are given
	 *
	 * @param segment
	 * @param minute
	 * @return
	 */
	protected boolean tollConditionSatisfied(SegmentIdentifier segment, long minute) {
		double segmentSpeed = 0;
		int carsOnSegment = 0;
		if(this.allNovLavs.containsKey(segment) && this.allNovLavs.get(segment).getMinute() == minute) {
			segmentSpeed = this.allNovLavs.get(segment).getLav();
			carsOnSegment = this.allNovLavs.get(segment).getNov();
		}

		boolean isAccident = false;
		if(this.allAccidents.containsKey(segment)) {
			isAccident = this.allAccidents.get(segment).active(minute);
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug(segment + " => segmentSpeed: " + segmentSpeed + "\tcarsOnSegment: " + carsOnSegment);
		}

		return segmentSpeed < MAX_SPEED_FOR_TOLL && carsOnSegment > MIN_CARS_FOR_TOLL && !isAccident;
	}

	@Override
	public void cleanup() {
		this.timer.stop();
		LOG.debug("TollNotificationBolt was running for %d seconds", this.timer.getElapsedTimeSecs());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, FIELDS_OUTGOING_TOLL_ASSESSMENT);

		declarer.declareStream(TopologyControl.TOLL_NOTIFICATION_STREAM_ID, FIELDS_OUTGOING_TOLL_NOTIFICATION);

	}

}
