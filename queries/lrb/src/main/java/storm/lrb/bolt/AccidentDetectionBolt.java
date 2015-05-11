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
package storm.lrb.bolt;

/*
 * #%L
 * lrb
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Collections;
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
import storm.lrb.tools.TupleHelpers;





/**
 * This bolt registers every stopped vehicle. If an accident was detected it emits accident information for further
 * processing.
 * 
 * Each AccidentDetectionBolt is responsible to check one assigned xway.
 * 
 * The description in the LRB paper isn't very helpful as the accident section doesn't describe accidents
 * completely:<blockquote> An accident occurs when two vehicles are "stopped" at the same position at the same time. A
 * vehicle is stopped when it reports the same position in 4 consecutive position reports. Once an accident occurs in a
 * given segment, traf- fic proceeds in that segment at a reduced speed determined by the traffic spacing model.
 * </blockquote> Below in the streaming description for accidents it gets clear what an accident involves: <blockquote>A
 * stream processing system should detect an accident on a given segment whenever two or more vehicles are stopped in
 * that segment at the same lane and position</blockquote>
 */
public class AccidentDetectionBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5537727428628598519L;
	private static final Logger LOG = LoggerFactory.getLogger(AccidentDetectionBolt.class);
	public static final Fields FIELDS_OUTGOING = new Fields(TopologyControl.POS_REPORT_FIELD_NAME,
		TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.ACCIDENT_INFO_FIELD_NAME);
	public static final Fields FIELDS_INCOMING = new Fields(TopologyControl.POS_REPORT_FIELD_NAME);
	
	/**
	 * Holds information about which car has been detected to be stopped how many times (>1) at each segment (identified
	 * by its id). An accident is defined to have occured after two vehicles has been detected to be stopped in 4
	 * consequtive position reports at the same position.
	 * 
	 * {segment id} x ({vehicle id} x {vehicle stop count})
	 */
	/*
	 * internal implementation notes: - keep one complex (but not necessarily slow) collection rather than be obliged to
	 * keep multiple collections in sync
	 */
	private final Map<Integer, Map<Integer, Integer>> stopInformationPerPosition = new HashMap<Integer, Map<Integer, Integer>>();
	/**
	 * Due to the fact that the {@link Accident} class manages a lot of information, it is necessary to reference it in
	 * a proper collection. This one holds {@code Position x (Lane x Accicent)}.
	 */
	private final Map<Integer, Map<Integer, Accident>> accidentsPerPosition = new HashMap<Integer, Map<Integer, Accident>>();
	
	private final int processed_xway;
	
	private OutputCollector collector;
	
	public AccidentDetectionBolt(int xway) {
		processed_xway = xway;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		if(TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("emit all accidents");
			emitCurrentAccidents();
			return;
		}
		
		PosReport report = (PosReport)tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
		
		// first process the tuple and put values into the collection, ...
		if(report == null) {
			LOG.warn("report is null, ackknowledging tuple and skipping");
		} else {
			if(report.getCurrentSpeed() == 0) {
				recordStoppedCar(report);
			} else {
				Map<Integer, Integer> vehicleStopInformationMap = this.stopInformationPerPosition.get(report
					.getPosition());
				if(vehicleStopInformationMap.containsKey(report.getVehicleIdentifier())) {
					// stopped car is moving again so check if you can clear accident
					LOG.debug("car is moving again; position report: %s", report);
					checkIfAccidentIsOver(report);
				}
			}
		}
		
		// ...then evaluate the collection
		collector.ack(tuple);
		
	}
	
	/**
	 * Only invoke if the accident at the position denoted by {@code report} can be cleared.
	 * 
	 * @param report
	 */
	private void checkIfAccidentIsOver(PosReport report) {
		// remove car from accidentcars
		Integer accidentPosition = report.getPosition();
		Map<Integer, Integer> vehicleStopInformationMap = this.stopInformationPerPosition.get(accidentPosition);
		
		Set<Integer> stoppedCarsAtPosition = vehicleStopInformationMap.keySet();
		
		if(stoppedCarsAtPosition.size() == 2) {
			// there has been an accident
			Map<Integer, Accident> laneAccidentMap = this.accidentsPerPosition.get(accidentPosition);
			Accident accidentinfo = laneAccidentMap.get(report.getLane());
			accidentinfo.setOver(report.getTime());
			
			LOG.info("accident is over: %s", accidentinfo);
			
			emitAccidentAtPosition(accidentPosition);
			laneAccidentMap.remove(report.getLane());
			if(laneAccidentMap.isEmpty()) {
				accidentsPerPosition.remove(accidentPosition);
			}
		}
	}
	
	private void recordStoppedCar(PosReport report) {
		int position = report.getPosition();
		Map<Integer, Integer> vehicleStopMap = stopInformationPerPosition.get(position);
		
		if(vehicleStopMap == null) {
			vehicleStopMap = new HashMap<Integer, Integer>();
			stopInformationPerPosition.put(position, vehicleStopMap);
		}
		Integer vehicleStopCount = vehicleStopMap.get(report.getVehicleIdentifier());
		if(vehicleStopCount == null) {
			vehicleStopCount = 1;
			vehicleStopMap.put(report.getVehicleIdentifier(), vehicleStopCount);
		} else {
			vehicleStopCount += 1;
		}
		vehicleStopMap.put(report.getVehicleIdentifier(), vehicleStopCount);
		
		if(vehicleStopCount >= 4) {// 4 consecutive stops => accident car
			// add or update accident
			updateAccident(report);
			
		}
		
	}
	
	private void updateAccident(PosReport report) {
		Map<Integer, Integer> vehicleStopMap = stopInformationPerPosition.get(report.getPosition());
		
		Set<Integer> accidentVehicleIdentifiers = new HashSet<Integer>();
		for(Integer vehicleIdentifier : vehicleStopMap.keySet()) {
			Integer vehicleStopCount = vehicleStopMap.get(vehicleIdentifier); // should be never null
			if(vehicleStopCount >= 4) {
				accidentVehicleIdentifiers.add(vehicleIdentifier);
			}
		}
		
		if(accidentVehicleIdentifiers.size() >= 2) {
			// accident at position
			Map<Integer, Accident> laneAccidentMap = accidentsPerPosition.get(report.getPosition());
			if(laneAccidentMap == null) {
				laneAccidentMap = new HashMap<Integer, Accident>();
				accidentsPerPosition.put(report.getPosition(), laneAccidentMap);
			}
			Accident laneAccident = laneAccidentMap.get(report.getLane());
			if(laneAccident == null) {
				laneAccident = new Accident(report);
				laneAccidentMap.put(report.getLane(), laneAccident);
				LOG.debug("emitting new accident: %s", laneAccident);
				laneAccident.getInvolvedCars().addAll(accidentVehicleIdentifiers);
			} else {
				LOG.debug("update accident: %s", laneAccident);
				laneAccident.getInvolvedCars().add(report.getVehicleIdentifier());
			}
		}
	}
	
	// emit all current accidents
	private void emitCurrentAccidents() {
		for(Map<Integer, Accident> laneAccidentMap : accidentsPerPosition.values()) {
			for(Accident accident : laneAccidentMap.values()) {
				emitAccident(accident);
			}
		}
	}
	
	private void emitAccident(Accident accident) {
		// emit accidents (for every affected segment)
		Set<SegmentIdentifier> segmensts = accident.getInvolvedSegs();
		for(SegmentIdentifier xsd : segmensts) {
			AccidentImmutable acc = new AccidentImmutable(accident);
			collector.emit(new Values(xsd, acc));
		}
	}
	
	/**
	 * emit newly detected accident at {@code position}
	 * 
	 * @param position
	 */
	private void emitAccidentAtPosition(Integer position) {
		LOG.debug("emmitting new or over accident on position %s", position);
		if(accidentsPerPosition.isEmpty()) {
			return;
		}
		Map<Integer, Accident> laneAccidentMap = accidentsPerPosition.get(position);
		if(laneAccidentMap == null) {
			return;
		}
		for(Accident accident : laneAccidentMap.values()) {
			emitAccident(accident);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(FIELDS_OUTGOING);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
		return conf;
	}
	
	@Override
	public String toString() {
		return "AccidentDetectionBolt \n [stoppedCarsPerXSegDir=" + this.stopInformationPerPosition
			+ ",\n allAccidentPositions=" + accidentsPerPosition + "]";
	}
	
	public Map<Integer, Map<Integer, Accident>> getAccidentsPerPosition() {
		return Collections.unmodifiableMap(accidentsPerPosition);
	}
	
	public Map<Integer, Map<Integer, Integer>> getStopInformationPerPosition() {
		return Collections.unmodifiableMap(stopInformationPerPosition);
	}
}
