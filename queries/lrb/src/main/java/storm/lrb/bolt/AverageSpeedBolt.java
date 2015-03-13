package storm.lrb.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.AvgVehicleSpeeds;
import storm.lrb.model.PosReport;
import storm.lrb.model.Time;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * This bolt computes the average speed of a vehicle in a given segment and direction and emits these every minute.
 * 
 */
public class AverageSpeedBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(AverageSpeedBolt.class);
	
	/**
	 * contains the time of the last reports of each segment (xsd, timeinsec)
	 */
	private final Map<SegmentIdentifier, Integer> timeOfLastReportsMap;
	/**
	 * contains all avgs of each vehicle driving in the given segment for the current minute. gets resetted every minute
	 * (xsd -> Map(minute ->avgsvehiclespeeds))
	 * 
	 */
	private final Map<SegmentIdentifier, AvgVehicleSpeeds> avgSpeedsMap; // xsd
	// =>
	// List<avg
	// vehicles
	// speeds>
	
	private volatile Integer lastEmitMinute = 0;
	private OutputCollector collector;
	private int processed_xway = -1;
	
	public AverageSpeedBolt(int xway) {
		this.timeOfLastReportsMap = new HashMap<SegmentIdentifier, Integer>();
		this.avgSpeedsMap = new HashMap<SegmentIdentifier, AvgVehicleSpeeds>();
		this.processed_xway = xway;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		PosReport pos = (PosReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
		
		SegmentIdentifier accidentIdentifier = new SegmentIdentifier(pos.getSegmentIdentifier().getxWay(), pos
			.getSegmentIdentifier().getSegment(), pos.getSegmentIdentifier().getDirection());
		
		int curminute = Time.getMinute(pos.getTime());
		
		// synchronized (lastEmitMinute) {
		// if a new minute starts emit all previous accumulated avgs
		if(this.lastEmitMinute < curminute) {
			this.lastEmitMinute = curminute;
			this.emitAllAndRemove(curminute - 1);
			
		}
		// }
		
		Integer timeOfLastReports = this.timeOfLastReportsMap.get(accidentIdentifier);
		if(timeOfLastReports == null) {
			timeOfLastReports = curminute;
			this.timeOfLastReportsMap.put(accidentIdentifier, timeOfLastReports);
		}
		
		AvgVehicleSpeeds lastSpeeds = this.avgSpeedsMap.get(accidentIdentifier);
		// synchronized(lastSpeeds){
		if(lastSpeeds == null) {
			lastSpeeds = new AvgVehicleSpeeds();
			this.avgSpeedsMap.put(accidentIdentifier, lastSpeeds);
		}
		
		this.timeOfLastReportsMap.put(accidentIdentifier, curminute);
		lastSpeeds.addVehicleSpeed(pos.getVehicleIdentifier(), pos.getCurrentSpeed());
		// }
		
		this.collector.ack(tuple);
	}
	
	private void emitAllAndRemove(int minute) {
		
		Set<SegmentIdentifier> segmentList = this.avgSpeedsMap.keySet();
		
		for(SegmentIdentifier xsd : segmentList) {
			AvgVehicleSpeeds lastSpeeds = this.avgSpeedsMap.get(xsd);
			if(lastSpeeds != null) {
				this.collector.emit(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, new Values(this.processed_xway, xsd,
					lastSpeeds.vehicleCount(), lastSpeeds.speedAverage(), minute));
				this.avgSpeedsMap.put(xsd, new AvgVehicleSpeeds());
			}
			
		}
		
	}
	
	private void emitAndRemove(SegmentIdentifier xsd, int minute) {
		
		AvgVehicleSpeeds lastSpeeds = this.avgSpeedsMap.get(xsd);
		if(lastSpeeds != null) {
			synchronized(lastSpeeds) {
				this.collector.emit(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, new Values(this.processed_xway, xsd,
					lastSpeeds.vehicleCount(), lastSpeeds.speedAverage(), minute));
				this.avgSpeedsMap.put(xsd, new AvgVehicleSpeeds());
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, new Fields(
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.CAR_COUNT_FIELD_NAME, TopologyControl.AVERAGE_SPEED_FIELD_NAME,
			TopologyControl.MINUTE_FIELD_NAME));
	}
	
}
