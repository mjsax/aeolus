/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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
import storm.lrb.tools.Helper;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.lrb.toll.TollDataStore;
import de.hub.cs.dbis.lrb.types.AbstractLRBTuple;
import de.hub.cs.dbis.lrb.types.DailyExpenditureRequest;
import de.hub.cs.dbis.lrb.util.Constants;





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
	
	private transient TollDataStore dataStore;
	
	private OutputCollector collector;
	
	/**
	 * 
	 */
	public DailyExpenditureBolt() {}
	
	public TollDataStore getDataStore() {
		return this.dataStore;
	}
	
	/**
	 * initializes the used {@link TollDataStore} using the string specified as value to the
	 * {@link Helper#TOLL_DATA_STORE_CONF_KEY} map key.
	 * 
	 * @param conf
	 * @param context
	 * @param collector
	 */
	/*
	 * internal implementation notes: - due to the fact that storm is incapable of serializing Class property, a String
	 * has to be passed in conf
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		@SuppressWarnings("unchecked")
		String tollDataStoreClass = (String)conf.get(Helper.TOLL_DATA_STORE_CONF_KEY);
		try {
			this.dataStore = (TollDataStore)Class.forName(tollDataStoreClass).newInstance();
		} catch(InstantiationException ex) {
			throw new RuntimeException(String.format("The data store instance '%s' could not be initialized (see "
				+ "nested exception for details)", this.dataStore), ex);
		} catch(IllegalAccessException ex) {
			throw new RuntimeException(String.format("The data store instance '%s' could not be initialized (see "
				+ "nested exception for details)", this.dataStore), ex);
		} catch(ClassNotFoundException ex) {
			throw new RuntimeException(String.format("The data store instance '%s' could not be initialized (see "
				+ "nested exception for details)", this.dataStore), ex);
		}
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		Fields fields = tuple.getFields();
		
		if(fields.contains(TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME)) {
			
			DailyExpenditureRequest exp = (DailyExpenditureRequest)tuple
				.getValueByField(TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME);
			int vehicleIdentifier = exp.getVid();
			Values values;
			Integer toll = this.dataStore.retrieveToll(exp.getXWay(), exp.getDay(), vehicleIdentifier);
			if(toll != null) {
				LOG.debug("ExpenditureRequest: found vehicle identifier %d", vehicleIdentifier);
				
				// LOG.debug("3, %d, %d, %d, %d", exp.getTime(), exp.getTimer().getOffset(), exp.getQueryIdentifier(),
				// toll);
				
				values = new Values(AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, exp.getTime(), exp.getQid(), toll);
			} else {
				values = new Values(AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, exp.getTime(), exp.getQid(),
					Constants.INITIAL_TOLL);
				
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
