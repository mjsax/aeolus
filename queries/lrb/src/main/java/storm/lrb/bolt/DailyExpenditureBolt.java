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

import java.util.Map;

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
import backtype.storm.utils.Utils;





/**
 * Stub for daily expenditure queries. Responds to {@link DailyExpenditureRequest}s with tuple in the form of (Type = 3,
 * Time (specifying the time that d was emitted), Emit (specifying the time the query response is emitted), QID
 * (identifying the query that issued the request), Bal (the sum of all tolls from expressway x on day n that were
 * charged to the vehi- cle’s account). Reads from {@link TopologyControl#DAILY_EXPEDITURE_REQUESTS_STREAM_ID} and emits
 * tuple on {@link Utils#DEFAULT_STREAM_ID}.
 * 
 * @TODO either use external distributed database to keep historic data or load it into memory
 * 
 */
public class DailyExpenditureBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DailyExpenditureBolt.class);
	
	private final TollDataStore dataStore;
	
	private OutputCollector collector;
	
	public DailyExpenditureBolt(TollDataStore dataStore) {
		this.dataStore = dataStore;
	}
	
	public TollDataStore getDataStore() {
		return dataStore;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		// TODO read histfile with csvreader or connect to db
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		Fields fields = tuple.getFields();
		
		if(fields.contains(TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME)) {
			
			DailyExpenditureRequest exp = (DailyExpenditureRequest)tuple
				.getValueByField(TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME);
			int vehicleIdentifier = exp.getVehicleIdentifier();
			Values values;
			Integer toll = this.dataStore.retrieveToll(exp.getxWay(), exp.getDay(), vehicleIdentifier);
			if(toll != null) {
				LOG.debug("ExpenditureRequest: found vehicle identifier %d", vehicleIdentifier);
				
				LOG.debug("3, %d, %d, %d, %d", exp.getCreated(), exp.getTimer().getOffset(), exp.getQueryIdentifier(),
					toll);
				
				values = new Values(LRBtuple.TYPE_DAILY_EXPEDITURE, exp.getCreated(), exp.getTimer().getOffset(),
					exp.getQueryIdentifier(), toll);
			} else {
				values = new Values(LRBtuple.TYPE_DAILY_EXPEDITURE, exp.getCreated(), exp.getTimer().getOffset(),
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
