package storm.lrb.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.Accident;
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.PosReport;
import storm.lrb.model.StoppedVehicle;
import storm.lrb.tools.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * This bolt registers every stopped vehicle. If an accident was detected it emits accident information for further
 * processing.
 * 
 * Each AccidentDetectionBolt is responsible to check one assigned xway.
 * 
 * The accident detection is based on
 * 
 */
public class AccidentDetectionBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(AccidentDetectionBolt.class);
	
	/**
	 * holds vids of stoppedcars (keyed on position)
	 */
	private final Map<Integer, HashSet<Integer>> stoppedCarsPerPosition = new HashMap<Integer, HashSet<Integer>>();
	/**
	 * holds all accident infos keyed on postion
	 */
	private final Map<Integer, Accident> allAccidentPositions = new HashMap<Integer, Accident>();
	/**
	 * holds cnts of stops for each vehicle
	 */
	private final Map<Integer, StoppedVehicle> stoppedCars = new HashMap<Integer, StoppedVehicle>();
	/**
	 * map of all accident cars and the position of the accident
	 */
	private final Map<Integer, Integer> allAccidentCars = new HashMap<Integer, Integer>();
	
	private final int processed_xway;
	
	private OutputCollector collector;
	
	public AccidentDetectionBolt(int xway) {
		this.processed_xway = xway;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		if(TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("emit all accidents");
			this.emitCurrentAccidents();
			return;
		}
		
		PosReport report = (PosReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
		
		if(report != null && report.getCurrentSpeed() == 0) {
			
			this.recordStoppedCar(report);
			
		} else if(report != null && this.allAccidentCars.containsKey(report.getVehicleIdentifier())) {
			// stopped car is moving again so check if you can clear accident
			LOG.debug("car is moving again; position report: %s", report);
			this.checkIfAccidentIsOver(report);
			
		}
		
		this.collector.ack(tuple);
		
	}
	
	private void checkIfAccidentIsOver(PosReport report) {
		// remove car from accidentcars
		int accposition = this.allAccidentCars.remove(report.getVehicleIdentifier());
		
		HashSet<Integer> cntstoppedCars = this.stoppedCarsPerPosition.get(accposition);
		
		cntstoppedCars.remove(report.getVehicleIdentifier());
		
		if(cntstoppedCars.size() == 1) {
			// only one accident car -> accident is over
			Accident accidentinfo = this.allAccidentPositions.get(accposition);
			accidentinfo.setOver(report.getTime());
			
			LOG.info("accident is over: %s", accidentinfo);
			
			this.emitCurrentAccident(accposition);
			this.allAccidentPositions.remove(accposition);
		}
		
		if(cntstoppedCars.isEmpty()) {
			// no stopped car left, remove position from stop watch list
			this.stoppedCarsPerPosition.remove(accposition);
		}
	}
	
	private void recordStoppedCar(PosReport report) {
		
		StoppedVehicle stopVehicle = this.stoppedCars.get(report.getVehicleIdentifier());
		
		int cnt;
		if(stopVehicle == null) {
			stopVehicle = new StoppedVehicle(report);
			cnt = 1;
			this.stoppedCars.put(report.getVehicleIdentifier(), stopVehicle);
		} else {
			cnt = stopVehicle.recordStop(report);
		}
		
		if(cnt >= 4) {// 4 consecutive stops => accident car
			this.allAccidentCars.put(report.getVehicleIdentifier(), report.getPosition());
			// add or update accident
			this.updateAccident(report, stopVehicle);
			
		}
		
	}
	
	private void updateAccident(PosReport report, StoppedVehicle stopVehicle) {
		HashSet<Integer> accCarCnt = this.stoppedCarsPerPosition.get(stopVehicle.getPosition());
		
		if(accCarCnt == null) {
			accCarCnt = new HashSet<Integer>();
			this.stoppedCarsPerPosition.put(stopVehicle.getPosition(), accCarCnt);
		}
		accCarCnt.add(stopVehicle.getVid());
		
		if(accCarCnt.size() >= 2) { // accident at position
			Accident accidentinfo = this.allAccidentPositions.get(stopVehicle.getPosition());
			if(accidentinfo == null) {// new accident emit!!
				accidentinfo = new Accident(report);
				this.allAccidentPositions.put(stopVehicle.getPosition(), accidentinfo);
				accidentinfo.addAccVehicles(accCarCnt);
				LOG.info("new accident: %s", accidentinfo);
				this.emitCurrentAccident(stopVehicle.getPosition());
			} else {
				LOG.debug("update accident: %s", accidentinfo);
				accidentinfo.updateAccident(report);
			}
		}
	}
	
	// emit all current accidents
	private void emitCurrentAccidents() {
		
		if(this.allAccidentPositions.isEmpty()) {
			return;
		}
		
		for(Map.Entry<Integer, Accident> e : this.allAccidentPositions.entrySet()) {
			
			Accident accident = e.getValue();
			this.emitAccident(accident);
		}
		
	}
	
	private void emitAccident(Accident accident) {
		// emit accidents (for every affected segment)
		Set<SegmentIdentifier> segmensts = accident.getInvolvedSegs();
		for(SegmentIdentifier xsd : segmensts) {
			AccidentImmutable acc = new AccidentImmutable(accident);
			this.collector.emit(new Values(this.processed_xway, xsd.getxWay(), // xway
				xsd.getSegment(), // segment
				xsd.getDirection(), // dir
				acc));
		}
	}
	
	// emit newly detected accident
	private void emitCurrentAccident(Integer position) {
		LOG.debug("emmitting new or over accident on position %s", position);
		
		if(this.allAccidentPositions.isEmpty()) {
			return;
		}
		Accident accident = this.allAccidentPositions.get(position);
		if(accident == null) {
			return;
		}
		this.emitAccident(accident);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.ACCIDENT_INFO_FIELD_NAME,
			TopologyControl.VEHICLE_ID_FIELD_NAME));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
		return conf;
	}
	
	@Override
	public String toString() {
		return "AccidentDetectionBolt \n [stoppedCarsPerXSegDir=" + this.stoppedCarsPerPosition
			+ ",\n allAccidentPositions=" + this.allAccidentPositions + ", \n allAccidentCars=" + this.allAccidentCars
			+ "]";
	}
	
}
