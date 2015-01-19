package storm.lrb.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

import storm.lrb.model.Accident;
import storm.lrb.model.AccidentImmutable;
import storm.lrb.model.StoppedVehicle;
import storm.lrb.model.PosReport;

import storm.lrb.tools.TupleHelpers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This bolt registers every stopped vehicle. If an accident was detected it
 * emits accident information for further processing.
 * 
 * Each AccidentDetectionBolt is responsible to check one assigned xway.
 * 
 * The accident detection is based on 
 * 
 */
public class AccidentDetectionBolt extends BaseRichBolt {

	
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = Logger
			.getLogger(AccidentDetectionBolt.class);

	/**
	 * holds vids of stoppedcars (keyed on position)
	 */
	private ConcurrentHashMap<Integer, HashSet<Integer>> stoppedCarsPerPosition;
	/**
	 *  holds all accident infos keyed on postion
	 */
	private ConcurrentHashMap<Integer, Accident> allAccidentPositions;
	/**
	 *  holds cnts of stops for each vehicle
	 */
	private ConcurrentHashMap<Integer, StoppedVehicle> stoppedCars;
	/**
	 * map of all accident cars and the position of the accident
	 */
	private ConcurrentHashMap<Integer, Integer> allAccidentCars;

	private final int processed_xway;

	private OutputCollector collector;

	public AccidentDetectionBolt(int xway) {
		processed_xway = xway;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,OutputCollector collector) {
		this.collector = collector;
	
		this.stoppedCarsPerPosition = new ConcurrentHashMap<Integer, HashSet<Integer>>();
		this.allAccidentPositions = new ConcurrentHashMap<Integer, Accident>();
		this.allAccidentCars = new ConcurrentHashMap<Integer,Integer>();
		this.stoppedCars = new ConcurrentHashMap<Integer,StoppedVehicle>();
		
	}

	@Override
	public void execute(Tuple tuple) {
		
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("ACCIDENTBOLT: emit all accidents:");
			emitCurrentAccidents();
			return;
		}

		PosReport report = (PosReport) tuple.getValueByField("PosReport");

		if (report != null && report.getCurrentSpeed() == 0) {
			
			recordStoppedCar(report);
			
		} else if (report != null && allAccidentCars.containsKey(report.getVehicleIdentifier())) {
			// stopped car is moving again so check if you can clear accident
			LOG.debug("ACCDECT: car is moving again"+report);
			checkIfAccidentIsOver(report);

		}

		collector.ack(tuple);

	}

	private void checkIfAccidentIsOver(PosReport report) {
		//remove car from accidentcars
		int accposition = allAccidentCars.remove(report.getVehicleIdentifier());
		
		HashSet<Integer> cntstoppedCars = stoppedCarsPerPosition.get(accposition);
		
		cntstoppedCars.remove(report.getVehicleIdentifier());
		
		if (cntstoppedCars.size() == 1) {
			//only one accident car -> accident is over 
			Accident accidentinfo = allAccidentPositions.get(accposition);
			accidentinfo.setOver(report.getTime());
			
			LOG.info("ACCIDENTBOLT: accident is over:"+accidentinfo);
			
			emitCurrentAccident(accposition);
			allAccidentPositions.remove(accposition);
		}

		if (cntstoppedCars.isEmpty()) {
			//no stopped car left, remove position from stop watch list
			stoppedCarsPerPosition.remove(accposition);
		}
	}

	private void recordStoppedCar(PosReport report) {

		StoppedVehicle stopVehicle = stoppedCars.get(report.getVehicleIdentifier());

		int cnt;
		if (stopVehicle == null) {
			stopVehicle = new StoppedVehicle(report);
			cnt = 1;
			stoppedCars.put(report.getVehicleIdentifier(), stopVehicle);			
		} else
			cnt = stopVehicle.recordStop(report);

		if (cnt >= 4) {// 4 consecutive stops => accident car 
			allAccidentCars.put(report.getVehicleIdentifier(), report.getPosition());
			//add or update accident
			updateAccident(report, stopVehicle);

		}

	}

	private void updateAccident(PosReport report, StoppedVehicle stopVehicle) {
		HashSet<Integer> accCarCnt = stoppedCarsPerPosition.get(stopVehicle.getPosition());
		
		if(accCarCnt==null){
			accCarCnt = new HashSet<Integer>();
			stoppedCarsPerPosition.put(stopVehicle.getPosition(), accCarCnt);
		}
		accCarCnt.add(stopVehicle.getVid());
		
		if (accCarCnt.size() >= 2) { // accident at position
			Accident accidentinfo = allAccidentPositions.get(stopVehicle.getPosition());
			if (accidentinfo == null) {// new accident emit!!
				accidentinfo = new Accident(report);
				allAccidentPositions.put(stopVehicle.getPosition(), accidentinfo);
				accidentinfo.addAccVehicles(accCarCnt);
				LOG.info("ACCIDENTBOLT: new accident" + accidentinfo);
				emitCurrentAccident(stopVehicle.getPosition());
			} else {
				LOG.debug("ACCIDENTBOLT: update accident"+ accidentinfo);
				accidentinfo.updateAccident(report);
			}
		}
	}

	

	//emit all current accidents
	private void emitCurrentAccidents() {

		if (allAccidentPositions.isEmpty()){
			return;
		}

		for (Map.Entry<Integer, Accident> e : allAccidentPositions.entrySet()) {

			Accident accident = e.getValue();
			emitAccident(accident);
		}

	}
	
	private void emitAccident(Accident accident) {
		// emit accidents (for every affected segment)
		Set<SegmentIdentifier> segmensts = accident.getInvolvedSegs();
		for (SegmentIdentifier xsd : segmensts) {
			AccidentImmutable acc = new AccidentImmutable(accident);
			collector.emit(new Values(processed_xway, 
				xsd.getxWay(), //xway
				xsd.getSegment(), //segment
				xsd.getDirection(), //dir
				acc));
		}
	}

	//emit newly detected accident
	private void emitCurrentAccident(Integer position) {
		LOG.debug("emmitting new or over accident on position: "+ position);

		if (allAccidentPositions.isEmpty()) {
			return;
		}
		Accident accident = allAccidentPositions.get(position);
		if (accident == null) {
			return;
		}
		emitAccident(accident);
	}


	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("xway","dir","xd", "xsd", "accidentInfo"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
		return conf;
	}
	
	@Override
	public String toString() {
		return "AccidentDetectionBolt \n [stoppedCarsPerXSegDir="
				+ stoppedCarsPerPosition + ",\n allAccidentPositions="
				+ allAccidentPositions + ", \n allAccidentCars=" + allAccidentCars
				+ "]";
	}

	
}
