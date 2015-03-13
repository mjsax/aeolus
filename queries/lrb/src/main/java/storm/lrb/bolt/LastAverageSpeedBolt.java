package storm.lrb.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * 
 * 
 * this bolt computes the last average vehicle speed (LAV) of the preceeding minute and holds the total count of cars.
 */
public class LastAverageSpeedBolt extends BaseRichBolt {
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(LastAverageSpeedBolt.class);
	protected static final int TOTAL_MINS = 200;
	/**
	 * map xsd to list of avg speeds for every segment and minute (TODO: change to hold only last five minutes)
	 */
	private final Map<Triple<Integer, Integer, Integer>, List<Double>> listOfavgs;
	
	private final Map<Triple<Integer, Integer, Integer>, List<Integer>> listOfVehicleCounts;
	
	private OutputCollector collector;
	
	public LastAverageSpeedBolt() {
		this.listOfavgs = new HashMap<Triple<Integer, Integer, Integer>, List<Double>>();
		this.listOfVehicleCounts = new HashMap<Triple<Integer, Integer, Integer>, List<Integer>>();
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		int minuteOfTuple = tuple.getIntegerByField(TopologyControl.MINUTE_FIELD_NAME);
		int xway = tuple.getIntegerByField(TopologyControl.XWAY_FIELD_NAME);
		int seg = tuple.getIntegerByField(TopologyControl.SEGMENT_FIELD_NAME);
		int dir = tuple.getIntegerByField(TopologyControl.DIRECTION_FIELD_NAME);
		int carcnt = tuple.getIntegerByField(TopologyControl.CAR_COUNT_FIELD_NAME);
		double avgs = tuple.getDoubleByField(TopologyControl.AVERAGE_SPEED_FIELD_NAME);
		
		Triple<Integer, Integer, Integer> segmentKey = new MutableTriple<Integer, Integer, Integer>(xway, seg, dir);
		List<Double> latestAvgSpeeds = this.listOfavgs.get(segmentKey);
		List<Integer> latestCarCnt = this.listOfVehicleCounts.get(segmentKey);
		
		if(latestAvgSpeeds == null) {
			// initialize the list for the full time so we can use the indices to keep track of time/speed
			latestAvgSpeeds = new ArrayList<Double>(Collections.nCopies(TOTAL_MINS, 0.0));// new ArrayList<Double>();
			this.listOfavgs.put(segmentKey, latestAvgSpeeds);
		}
		
		latestAvgSpeeds.add(minuteOfTuple, avgs);
		
		if(latestCarCnt == null) {
			latestCarCnt = new ArrayList<Integer>(Collections.nCopies(TOTAL_MINS, 0));
			this.listOfVehicleCounts.put(segmentKey, latestCarCnt);
			
		}
		
		latestCarCnt.add(minuteOfTuple, carcnt);
		this.emitLav(segmentKey, latestCarCnt, latestAvgSpeeds, minuteOfTuple);
		
		this.collector.ack(tuple);
		
	}
	
	/**
	 * loops over the preceeding five items in our list of speedvalues. and calculates lav
	 * 
	 * @param speedValues
	 * @param minute
	 * @return lav for the currentminute
	 */
	protected double calcLav(List<Double> speedValues, int minute) {
		double sumOfAvgSpeeds = 0;
		int divide = 0;
		for(int i = Math.max(minute - 4, 1); i <= minute; i++) {
			sumOfAvgSpeeds += speedValues.get(i);
			if(speedValues.get(i) != 0.0) {
				divide++;
			}
		}
		return sumOfAvgSpeeds / Math.max(divide, 1);
		
	}
	
	private void emitLav(Triple<Integer, Integer, Integer> xsd, List<Integer> vehicleCounts, List<Double> speedValues, int minute) {
		
		double speedAverage = this.calcLav(speedValues, minute);
		int segmentCarCount = vehicleCounts.get(minute);
		
		this.collector.emit(new Values(xsd.getLeft(), xsd.getMiddle(), xsd.getRight(), segmentCarCount, speedAverage,
			minute + 1));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, new Fields(
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
			TopologyControl.NUMBER_OF_VEHICLES_FIELD_NAME, TopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME,
			TopologyControl.MINUTE_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME));
	}
	
}
