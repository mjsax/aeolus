package storm.lrb;



/**
 * 
 * @author richter
 */
public class TopologyControl {
	
	public final static String TOPOLOGY_NAME = "test_lrb";
	
	/*
	 * internal implementation notes: - use java class name notation in order to minimize changes if bolts are actually
	 * implemented as classes one day
	 */
	public final static String SPLIT_STREAM_BOLT_NAME = "SplitStreamBolt";
	public final static String AVERAGE_SPEED_BOLT_NAME = "AvergageSpeedBolt";
	public final static String LAST_AVERAGE_SPEED_BOLT_NAME = "LastAverageSpeedBolt";
	public final static String TOLL_NOTIFICATION_BOLT_NAME = "TollNotificationBolt";
	public final static String ACCIDENT_DETECTION_BOLT_NAME = "AccidentDetectionBolt";
	public final static String ACCIDENT_NOTIFICATION_BOLT_NAME = "AccidentNotificationBolt";
	public final static String ACCIDENT_FILE_WRITER_BOLT_NAME = "AccidentFileWriterBolt";
	public final static String TOLL_FILE_WRITER_BOLT_NAME = "TollFileWriterBolt";
	public final static String ACCOUNT_BALANCE_BOLT_NAME = "AccountBalanceBolt";
	public final static String ACCOUNT_BALANCE_FILE_WRITER_BOLT_NAME = "AccountBalanceFileWriterBolt";
	public final static String DAILY_EXPEDITURE_BOLT_NAME = "DailyExpenditureBolt";
	public final static String DAILY_EXPEDITURE_FILE_WRITER_BOLT_NAME = "DailyExpeditureFileWriterBolt";
	
	public final static String POS_REPORTS_STREAM_ID = "PosReportStream";
	public final static String ACCOUNT_BALANCE_REQUESTS_STREAM_ID = "AccountBalanceRequestsStream";
	public final static String DAILY_EXPEDITURE_REQUESTS_STREAM_ID = "DailyExpeditureRequestsStream";
	public final static String SPOUT_STREAM_ID = "SpoutStream";
	public final static String TOLL_ASSESSMENT_STREAM_ID = "TollAssessment_";
	public final static String TOLL_NOTIFICATION_STREAM_ID = "TollNotification_";
	public final static String TRAVEL_TIME_REQUEST_STREAM_ID = "TTEstRequests";
	public final static String LAST_AVERAGE_SPEED_STREAM_ID = "LastAverageSpeedStream";
	
	public final static String START_SPOUT_NAME = "StartSpout";
	
	/**
	 * The identifier of the XWay field.
	 */
	public final static String XWAY_FIELD_NAME = "xway";
	public final static String SEGMENT_FIELD_NAME = "seg";
	public final static String DIRECTION_FIELD_NAME = "dir";
	public final static String VEHICLE_ID_FIELD_NAME = "vid";
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
	public final static String AVERAGE_SPEED_FIELD_NAME = "avgs";
	public final static String ACCIDENT_INFO_FIELD_NAME = "accidentInfo";
	public final static String ACCIDENT_NOTIFICATION_FIELD_NAME = "accnotification";
	public final static String TUPLE_FIELD_NAME = "tuple";
	public final static String TIMER_FIELD_NAME = "StormTimer";
	public final static String BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME = "balancenotification";
	
	private TopologyControl() {}
}
