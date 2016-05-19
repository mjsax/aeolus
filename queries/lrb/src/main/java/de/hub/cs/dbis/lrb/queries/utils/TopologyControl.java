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
package de.hub.cs.dbis.lrb.queries.utils;

import de.hub.cs.dbis.lrb.queries.LinearRoad;





/**
 * Names of Spouts, Bolts, fields/attributes, and streams used by {@link LinearRoad} and sub-queries.
 * 
 * @author richter
 * @author mjsax
 */
public class TopologyControl {
	
	// The identifier of the topology.
	public final static String TOPOLOGY_NAME = "Linear-Road-Benchmark";
	
	// spouts
	public final static String SPOUT_NAME = "File-Spout";
	// helper bolts
	public final static String SPLIT_STREAM_BOLT_NAME = "Split-Stream-Bolt";
	// operators
	public final static String STOPPED_CARS_BOLT_NAME = "Stopped-Cars-Bolt";
	public final static String ACCIDENT_DETECTION_BOLT_NAME = "Accident-Detection-Bolt";
	public final static String ACCIDENT_NOTIFICATION_BOLT_NAME = "Accident-Notification-Bolt";
	public final static String COUNT_VEHICLES_BOLT_NAME = "Count-Vehicles-Bolt";
	public final static String AVERAGE_VEHICLE_SPEED_BOLT_NAME = "Average-Vehicle-Speed-Bolt";
	public final static String AVERAGE_SPEED_BOLT_NAME = "Average-Speed-Bolt";
	public final static String LATEST_AVERAGE_SPEED_BOLT_NAME = "Latest-Average-Speed-Bolt";
	public final static String TOLL_NOTIFICATION_BOLT_NAME = "Toll-Notification-Bolt";
	
	// sinks
	public final static String ACCIDENT_FILE_WRITER_BOLT_NAME = "Accident-File-Writer-Bolt";
	public final static String TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME = "Toll-Notifications-File-Writer-Bolt";
	public final static String TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME = "Toll-Assessments-File-Writer-Bolt";
	
	// streams
	// input
	public final static String POSITION_REPORTS_STREAM_ID = "pr";
	public final static String ACCOUNT_BALANCE_REQUESTS_STREAM_ID = "ab";
	public final static String DAILY_EXPEDITURE_REQUESTS_STREAM_ID = "de";
	public final static String TRAVEL_TIME_REQUEST_STREAM_ID = "tt";
	// output
	public final static String TOLL_NOTIFICATIONS_STREAM_ID = "tn";
	public final static String TOLL_ASSESSMENTS_STREAM_ID = "ta";
	// internal
	public final static String ACCIDENTS_STREAM_ID = "acc";
	public final static String CAR_COUNTS_STREAM_ID = "cnt";
	public final static String LAVS_STREAM_ID = "lav";
	
	/*
	 * The identifiers of tuple attributes.
	 */
	// General (all input and output tuples)
	public final static String TYPE_FIELD_NAME = "type";
	public final static String TIMESTAMP_FIELD_NAME = "timestamp";
	
	// Input (all)
	public final static String VEHICLE_ID_FIELD_NAME = "vid"; // also used in some output tuples
	// Position Report
	public final static String SPEED_FIELD_NAME = "spd"; // also used in Toll Notification
	public final static String XWAY_FIELD_NAME = "xway";
	public final static String LANE_FIELD_NAME = "lane";
	public final static String DIRECTION_FIELD_NAME = "dir";
	public final static String SEGMENT_FIELD_NAME = "seg"; // also used in Accident Notification
	public final static String POSITION_FIELD_NAME = "pos";
	// Requests (all)
	public final static String QUERY_ID_FIELD_NAME = "qid";
	// Daily Expenditure Request
	public final static String DAY_FIELD_NAME = "day";
	// Travel Time Request
	public final static String START_SEGMENT_FIELD_NAME = "s-init";
	public final static String END_SEGMENT_FIELD_NAME = "s-end";
	public final static String DAY_OF_WEEK_FIELD_NAME = "dow";
	public final static String TIME_OF_DAY_FIELD_NAME = "tod";
	
	// Output (all)
	public final static String EMIT_FIELD_NAME = "emit";
	// Accident Notification
	// re-uses SEGMENT_FIELD_NAME and VEHICLE_ID_FIELD_NAME from PositionReport
	// Toll Notification
	// re-uses SPEED_FIELD_NAME from PositionReport
	public final static String TOLL_FIELD_NAME = "toll";
	
	// internal
	public final static String AVERAGE_VEHICLE_SPEED_FIELD_NAME = "avgvs";
	public final static String AVERAGE_SPEED_FIELD_NAME = "avgs";
	public final static String CAR_COUNT_FIELD_NAME = "cnt";
	public final static String LAST_AVERAGE_SPEED_FIELD_NAME = "lav";
	public final static String MINUTE_FIELD_NAME = "minute";
	
	private TopologyControl() {}
}
