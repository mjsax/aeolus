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
package storm.lrb;



/**
 * TODO
 * 
 * @author richter
 * @author mjsax
 */
public class TopologyControl {
	
	// The identifier of the topology.
	public final static String TOPOLOGY_NAME = "Linear Road Benchmark";
	
	// spouts
	public final static String SPOUT_NAME = "File Spout";
	// helper bolts
	public final static String SPLIT_STREAM_BOLT_NAME = "Split Stream Bolt";
	// operators
	public final static String ACCIDENT_DETECTION_BOLT_NAME = "Accident Detection Bolt";
	public final static String ACCIDENT_NOTIFICATION_BOLT_NAME = "Accident Notification Bolt";
	// sinks
	public final static String ACCIDENT_FILE_WRITER_BOLT_NAME = "Accident File Writer Bolt";
	
	// streams
	public final static String POSITION_REPORTS_STREAM = "pr";
	public final static String ACCOUNT_BALANCE_REQUESTS_STREAM = "ab";
	public final static String DAILY_EXPEDITURE_REQUESTS_STREAM = "de";
	public final static String TRAVEL_TIME_REQUEST_STREAM = "tt";
	
	// TODO check usage
	// bolts
	public final static String AVERAGE_SPEED_BOLT_NAME = "Avergage Speed Bolt";
	public final static String LAST_AVERAGE_SPEED_BOLT_NAME = "Last Average Speed Bolt";
	public final static String TOLL_NOTIFICATION_BOLT_NAME = "Toll Notification Bolt";
	public final static String TOLL_FILE_WRITER_BOLT_NAME = "Toll File Writer Bolt";
	public final static String ACCOUNT_BALANCE_BOLT_NAME = "AccountBalanceBolt";
	public final static String ACCOUNT_BALANCE_FILE_WRITER_BOLT_NAME = "Account Balance File Writer Bolt";
	public final static String DAILY_EXPEDITURE_BOLT_NAME = "DailyExpenditureBolt";
	public final static String DAILY_EXPEDITURE_FILE_WRITER_BOLT_NAME = "DailyExpeditureFileWriterBolt";
	// streams
	public final static String ACCIDENT_INFO_STREAM_ID = "AccidentInfoStream";
	public final static String TOLL_ASSESSMENT_STREAM_ID = "TollAssessmentStream";
	public final static String TOLL_NOTIFICATION_STREAM_ID = "TollNotificationStream";
	public final static String LAST_AVERAGE_SPEED_STREAM_ID = "LastAverageSpeedStream";
	
	/*
	 * The identifiers of tuple attributes.
	 */
	// General (input and output)
	public final static String TYPE_FIELD_NAME = "type";
	public final static String TIMESTAMP_FIELD_NAME = "timestamp";
	
	// Input (all)
	public final static String VEHICLE_ID_FIELD_NAME = "vid";
	// Position Report
	public final static String SPEED_FIELD_NAME = "speed";
	public final static String XWAY_FIELD_NAME = "xway";
	public final static String LANE_FIELD_NAME = "lane";
	public final static String DIRECTION_FIELD_NAME = "dir";
	public final static String SEGMENT_FIELD_NAME = "seg";
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
	
	// internal
	public final static String AVERAGE_VEHICLE_SPEED_FIELD_NAME = "avgvs";
	public final static String AVERAGE_SPEED_FIELD_NAME = "avgs";
	
	// TODO check if needed
	public final static String TOLL_ASSESSED_FIELD_NAME = "tollAssessed";
	public final static String POS_REPORT_FIELD_NAME = "PosReport";
	public final static String TOLL_NOTIFICATION_FIELD_NAME = "tollnotification";
	public final static String ACCOUNT_BALANCE_REQUEST_FIELD_NAME = "AccBalRequests";
	public final static String DAILY_EXPEDITURE_REQUEST_FIELD_NAME = "DaiExpRequests";
	public final static String TRAVEL_TIME_REQUEST_FIELD_NAME = "TTEstRequests";
	public final static String LAST_AVERAGE_SPEED_FIELD_NAME = "lav";
	public final static String MINUTE_FIELD_NAME = "minute";
	public final static String NUMBER_OF_VEHICLES_FIELD_NAME = "nov"; // @TODO: is that maybe the same as CAR_COUNT?
	public final static String EXPEDITURE_NOTIFICATION_FIELD_NAME = "expenditurenotification";
	public final static String CAR_COUNT_FIELD_NAME = "carcnt";
	public final static String ACCIDENT_NOTIFICATION_FIELD_NAME = "accnotification";
	public final static String TUPLE_FIELD_NAME = "tuple";
	public final static String ACCIDENT_INFO_FIELD_NAME = "accidentInfo";
	public final static String TIME_FIELD_NAME = "StormTimer";
	public final static String BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME = "balancenotification";
	
	private TopologyControl() {}
}
