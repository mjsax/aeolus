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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.DailyExpenditureRequest;
import storm.lrb.model.LRBtuple;
import storm.lrb.tools.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * Stub for daily expenditure queries. TODO either use external distributed database to keep historic data or load it
 * into memory
 * 
 */
public class DailyExpenditureBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DailyExpenditureBolt.class);
	
	private final String histFile;
	
	private final HashMap<Integer, HashMap<Pair<Integer, Integer>, Integer>> tollAccounts = new HashMap<Integer, HashMap<Pair<Integer, Integer>, Integer>>();
	
	private OutputCollector collector;
	
	public DailyExpenditureBolt(String file) {
		this.histFile = file;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		if(this.histFile.isEmpty()) {
			LOG.warn("no filename for historic data given.");
		}
		
		// TODO read histfile with csvreader or connect to db
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		Fields fields = tuple.getFields();
		
		if(fields.contains(TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME)) {
			
			DailyExpenditureRequest exp = (DailyExpenditureRequest)tuple
				.getValueByField(TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME);
			String out;
			int vehicleIdentifier = exp.getVehicleIdentifier();
			Values values;
			if(this.tollAccounts.containsKey(vehicleIdentifier)) {
				LOG.debug("ExpenditureRequest: found vehicle identifier %d", vehicleIdentifier);
				Pair<Integer, Integer> key = new MutablePair<Integer, Integer>(exp.getxWay(), exp.getDay());
				
				int toll = this.tollAccounts.get(exp.getVehicleIdentifier()).get(key);
				
				LOG.debug("3, %d, %d, %d, %d", exp.getTime(), exp.getTimer().getOffset(), exp.getQueryIdentifier(),
					toll);
				
				values = new Values(LRBtuple.TYPE_DAILY_EXPEDITURE, exp.getTime(), exp.getTimer().getOffset(),
					exp.getQueryIdentifier(), toll);
			} else {
				values = new Values(LRBtuple.TYPE_DAILY_EXPEDITURE, exp.getTime(), exp.getTimer().getOffset(),
					exp.getQueryIdentifier(), Constants.INITIAL_TOLL);
				
			}
			this.collector.emit(values);
		}
		this.collector.ack(tuple);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TopologyControl.EXPEDITURE_NOTIFICATION_FIELD_NAME));
	}
	
}
