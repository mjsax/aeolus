package storm.lrb.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

import storm.lrb.model.AvgVehicleSpeeds;
import storm.lrb.model.PosReport;

import storm.lrb.model.Time;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This bolt computes the average speed of a vehicle in a given segment and
 * direction and emits these every minute
 * 
 */
public class AvgsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger.getLogger(AvgsBolt.class);

	

	/**
	 * contains the time of the last reports of each segment
	 * (xsd, timeinsec)
	 */
	protected ConcurrentHashMap<String, Integer> timeOfLastReportsMap;
	/**
	 * contains all avgs of each vehicle driving in the given segment
	 * for the current minute. gets resetted every minute 
	 * (xsd -> Map(minute ->avgsvehiclespeeds))
	 * 
	 */
	protected ConcurrentHashMap<String, AvgVehicleSpeeds> avgSpeedsMap; // xsd
																		// =>
																		// List<avg
																		// vehicles
																		// speeds>

	private volatile Integer lastEmitMinute = 0;
	private OutputCollector collector;
	private int processed_xway = -1;

	public AvgsBolt(int xway) {
		timeOfLastReportsMap = new ConcurrentHashMap<String, Integer>();
		avgSpeedsMap = new ConcurrentHashMap<String, AvgVehicleSpeeds>();
		processed_xway = xway;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple tuple) {

		PosReport pos = (PosReport) tuple.getValueByField("PosReport");

		String xsd = pos.getXsd();

		int curminute = Time.getMinute(pos.getTime());

		
		//synchronized (lastEmitMinute) {
		
		// if a new minute starts emit all previous accumulated avgs
		if (lastEmitMinute < curminute) {
			lastEmitMinute = curminute;
			emitAllAndRemove(curminute - 1);
			
		}
		//}

		Integer timeOfLastReports = timeOfLastReportsMap.get(xsd);
		if (timeOfLastReports == null) {
			timeOfLastReports = curminute;
			timeOfLastReportsMap.put(xsd, timeOfLastReports);
		}

		AvgVehicleSpeeds lastSpeeds = avgSpeedsMap.get(xsd);
		// synchronized(lastSpeeds){
		if (lastSpeeds == null) {
			lastSpeeds = new AvgVehicleSpeeds();
			avgSpeedsMap.put(xsd, lastSpeeds);
		}

		timeOfLastReportsMap.put(xsd, curminute);
		lastSpeeds.addVehicleSpeed(pos.getVid(), pos.getSpd());
		// }

		collector.ack(tuple);
	}

	private void emitAllAndRemove(int minute) {

		Set<String> segmentList = avgSpeedsMap.keySet();

		for (String xsd : segmentList) {
			AvgVehicleSpeeds lastSpeeds = avgSpeedsMap.get(xsd);
			if (lastSpeeds != null) {
				collector.emit(new Values(processed_xway, xsd, lastSpeeds.vehicleCount(), lastSpeeds.speedAverage(), minute));
				avgSpeedsMap.replace(xsd, new AvgVehicleSpeeds());
			}

		}

	}

	private void emitAndRemove(String xsd, int minute) {

		AvgVehicleSpeeds lastSpeeds = avgSpeedsMap.get(xsd);
		if (lastSpeeds != null) {
			synchronized (lastSpeeds) {
				collector.emit(new Values(processed_xway, xsd, lastSpeeds.vehicleCount(), lastSpeeds.speedAverage(), minute));
				avgSpeedsMap.replace(xsd, new AvgVehicleSpeeds());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("xway", "xsd", "carcnt", "avgs", "minute"));
	}

}
