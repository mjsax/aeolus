package storm.lrb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

import storm.lrb.tools.Helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * 
 * this bolt computes the lav of the preceeding minute and
 * holds the total cnt of cars.
 */
public class LavBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger.getLogger(LavBolt.class);
	protected static final int TOTAL_MINS = 200;
	/**
	 * map xsd to list of avg speeds for every segment and minute
	 * (TODO: change to hold only last five minutes)
	 */
	protected ConcurrentHashMap<String, List<Double>> listOfavgs;

	/**
	 * 
	 */
	protected ConcurrentHashMap<String, List<Integer>> listOfVehicleCounts;
	

	protected Integer lastEmitMinute = 0;
	
	protected Integer currentMinute = 0;
	/**
	 * Holds all lavs and the car cnt
	 */

	private OutputCollector collector;
	private int processed_xway = -1;

	public LavBolt(int xway) {
		processed_xway=xway;
	
		this.listOfavgs = new ConcurrentHashMap<String, List<Double>>();
		this.listOfVehicleCounts = new ConcurrentHashMap<String, List<Integer>>();
	
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		int minuteOfTuple = tuple.getIntegerByField("minute");
		String xsd = tuple.getStringByField("xsd");
		int carcnt = tuple.getIntegerByField("carcnt");
		double avgs = tuple.getDoubleByField("avgs");

		List<Double> latestAvgSpeeds = listOfavgs.get(xsd);
		List<Integer> latestCarCnt = listOfVehicleCounts.get(xsd);
	
	
		if (latestAvgSpeeds == null) {
			//initialize the list for the full time so we can use the indices to keep track of time/speed
			latestAvgSpeeds =  new ArrayList(Collections.nCopies(TOTAL_MINS, 0.0));//new ArrayList<Double>();
			listOfavgs.put(xsd, latestAvgSpeeds);
		}

		latestAvgSpeeds.add(minuteOfTuple,avgs);
		
		if(latestCarCnt==null){
			latestCarCnt = new ArrayList(Collections.nCopies(TOTAL_MINS, 0));
			listOfVehicleCounts.put(xsd, latestCarCnt);
		
		}
	
		latestCarCnt.add(minuteOfTuple, carcnt);
		emitLav(xsd, latestCarCnt, latestAvgSpeeds, minuteOfTuple);
		
		collector.ack(tuple);

	}

	/**
	 * loops over the preceeding five items in our list of speedvalues.
	 * and calculates lav
	 * @param speedValues
	 * @param minute
	 * @return lav for the currentminute
	 */
	
	protected double calcLav(List<Double> speedValues, int minute) {
		double sumOfAvgSpeeds = 0;
		int divide = 0;
		for (int i = Math.max(minute-4, 1); i <= minute; i++) {
			sumOfAvgSpeeds += speedValues.get(i);
			if(speedValues.get(i)!=0.0) divide++;
		}
		return sumOfAvgSpeeds / Math.max(divide, 1);
		
	
	}
	

	private void emitLav(String xsd,  List<Integer> vehicleCounts, List<Double> speedValues,
			int minute) {
		
		double speedAverage = calcLav(speedValues, minute);
		int segmentCarCount = vehicleCounts.get(minute);
			
		collector.emit(new Values(Helper.getXwayFromXSD(xsd), Helper
				.getDirFromXSD(xsd), Helper.getXDfromXSD(xsd), xsd,
				segmentCarCount, speedAverage, minute+1));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("xway", "dir", "xd", "xsd", "nov", "lav","minute"));
	}

}
