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
    private static final Logger LOG = LoggerFactory
            .getLogger(AccidentDetectionBolt.class);

    /**
     * holds vids of stoppedcars (keyed on position)
     */
    private Map<Integer, HashSet<Integer>> stoppedCarsPerPosition;
    /**
     * holds all accident infos keyed on postion
     */
    private Map<Integer, Accident> allAccidentPositions;
    /**
     * holds cnts of stops for each vehicle
     */
    private Map<Integer, StoppedVehicle> stoppedCars;
    /**
     * map of all accident cars and the position of the accident
     */
    private Map<Integer, Integer> allAccidentCars;

    private final int processed_xway;

    private OutputCollector collector;

    public AccidentDetectionBolt(int xway) {
        processed_xway = xway;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, 
            TopologyContext context, 
            OutputCollector collector) {
        this.collector = collector;

        this.stoppedCarsPerPosition = new HashMap<Integer, HashSet<Integer>>();
        this.allAccidentPositions = new HashMap<Integer, Accident>();
        this.allAccidentCars = new HashMap<Integer, Integer>();
        this.stoppedCars = new HashMap<Integer, StoppedVehicle>();

    }

    @Override
    public void execute(Tuple tuple) {

        if (TupleHelpers.isTickTuple(tuple)) {
            LOG.debug("emit all accidents");
            emitCurrentAccidents();
            return;
        }

        PosReport report = (PosReport) tuple.getValueByField(
                TopologyControl.POS_REPORT_FIELD_NAME);

        if (report != null && report.getCurrentSpeed() == 0) {

            recordStoppedCar(report);

        } else if (report != null && allAccidentCars.containsKey(report.getVehicleIdentifier())) {
            // stopped car is moving again so check if you can clear accident
            LOG.debug("car is moving again; position report: %s", report);
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

            LOG.info("accident is over: %s", accidentinfo);

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
        } else {
            cnt = stopVehicle.recordStop(report);
        }

        if (cnt >= 4) {// 4 consecutive stops => accident car 
            allAccidentCars.put(report.getVehicleIdentifier(), report.getPosition());
            //add or update accident
            updateAccident(report, stopVehicle);

        }

    }

    private void updateAccident(PosReport report, StoppedVehicle stopVehicle) {
        HashSet<Integer> accCarCnt = stoppedCarsPerPosition.get(stopVehicle.getPosition());

        if (accCarCnt == null) {
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
                LOG.info("new accident: %s", accidentinfo);
                emitCurrentAccident(stopVehicle.getPosition());
            } else {
                LOG.debug("update accident: %s", accidentinfo);
                accidentinfo.updateAccident(report);
            }
        }
    }

    //emit all current accidents
    private void emitCurrentAccidents() {

        if (allAccidentPositions.isEmpty()) {
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
        LOG.debug("emmitting new or over accident on position %s", position);

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
        declarer.declare(new Fields(TopologyControl.XWAY_FIELD_NAME,
                TopologyControl.DIRECTION_FIELD_NAME,
                TopologyControl.SEGMENT_FIELD_NAME, 
                TopologyControl.ACCIDENT_INFO_FIELD_NAME,
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
        return "AccidentDetectionBolt \n [stoppedCarsPerXSegDir="
                + stoppedCarsPerPosition + ",\n allAccidentPositions="
                + allAccidentPositions + ", \n allAccidentCars=" + allAccidentCars
                + "]";
    }

}
