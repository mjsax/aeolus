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
package storm.lrb;



/**
 * Old stuff.
 * 
 * @author richter
 * @author mjsax
 */
public class TopologyControlOld {
	
	// // bolts
	public final static String ACCOUNT_BALANCE_BOLT_NAME = "AccountBalanceBolt";
	public final static String ACCOUNT_BALANCE_FILE_WRITER_BOLT_NAME = "AccountBalanceFileWriterBolt";
	public final static String DAILY_EXPEDITURE_BOLT_NAME = "DailyExpenditureBolt";
	public final static String DAILY_EXPEDITURE_FILE_WRITER_BOLT_NAME = "DailyExpeditureFileWriterBolt";
	// streams
	public final static String ACCIDENT_INFO_STREAM_ID = "AccidentInfoStream";
	public final static String LAST_AVERAGE_SPEED_STREAM_ID = "LastAverageSpeedStream";
	
	// TODO check if needed
	public final static String POS_REPORT_FIELD_NAME = "PosReport";
	public final static String TOLL_NOTIFICATION_FIELD_NAME = "tollnotification";
	public final static String ACCOUNT_BALANCE_REQUEST_FIELD_NAME = "AccBalRequests";
	public final static String DAILY_EXPEDITURE_REQUEST_FIELD_NAME = "DaiExpRequests";
	public final static String EXPEDITURE_NOTIFICATION_FIELD_NAME = "expenditurenotification";
	public final static String TUPLE_FIELD_NAME = "tuple";
	public final static String ACCIDENT_INFO_FIELD_NAME = "accidentInfo";
	public final static String BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME = "balancenotification";
	
	private TopologyControlOld() {}
}
