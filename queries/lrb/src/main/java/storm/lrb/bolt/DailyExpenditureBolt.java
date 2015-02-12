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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import storm.lrb.model.DaiExpRequest;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import storm.lrb.model.LRBtuple;
import storm.lrb.tools.Constants;

/**
 * Stub for daily expenditure queries. TODO either use external distributed
 * database to keep historic data or load it into memory
 * 
 */
public class DailyExpenditureBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	private final String histFile;
	
	private final HashMap<Integer, HashMap<Pair<Integer,Integer>, Integer>> tollAccounts = new HashMap<Integer, HashMap<Pair<Integer,Integer>, Integer>>();;
	private static final Logger LOG = Logger
			.getLogger(DailyExpenditureBolt.class);

	
	@Override
	public void prepare(Map conf, TopologyContext context,OutputCollector collector) {
		_collector = collector;
		if(histFile.isEmpty())
			LOG.warn("no filename for historic data given.");
		
		//TODO read histfile with csvreader or connect to db
		
	}
	
	 public DailyExpenditureBolt(String file) {
		   histFile = file;
		  }


	@Override
	public void execute(Tuple tuple) {

		Fields fields = tuple.getFields();
		
		if (fields.contains("DaiExpRequests")) {
			
			
			DaiExpRequest exp = (DaiExpRequest) tuple.getValueByField("DaiExpRequests");
			String out = "";
			int vehicleIdentifier = exp.getVehicleIdentifier();
			Values values;
			if(tollAccounts.containsKey(vehicleIdentifier)){
				LOG.debug(String.format("ExpenditureRequest: found vehicle identifier %d", vehicleIdentifier));
				Pair<Integer,Integer> key = new MutablePair<Integer,Integer>(exp.getSegmentIdentifier().getxWay(), exp.getDay());
				
				int toll = tollAccounts.get(exp.getVehicleIdentifier()).get(key);
				
				out = "3,"+exp.getTime()+","+exp.getEmitTime()+","+exp.getQueryIdentifier()+","+toll;
				
				
				values = new Values(
				LRBtuple.TYPE_DAILY_EXPEDITURE, 
				exp.getTime(), 
				exp.getEmitTime(), 
				exp.getQueryIdentifier(), 
				toll);
			}else {
				values = new Values(
				LRBtuple.TYPE_DAILY_EXPEDITURE, 
				exp.getTime(), 
				exp.getEmitTime(), 
				exp.getQueryIdentifier(), 
				Constants.INITIAL_TOLL);
				
			}
			_collector.emit(values);
		}
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("expenditurenotification"));
	}


}
