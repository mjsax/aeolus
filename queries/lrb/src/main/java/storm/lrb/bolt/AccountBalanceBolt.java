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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControlOld;
import storm.lrb.model.AccountBalance;
import storm.lrb.model.VehicleAccount;
import storm.lrb.tools.Helper;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.lrb.operators.TollNotificationBolt;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.AccountBalanceRequest;
import de.hub.cs.dbis.lrb.types.PositionReport;





/**
 * 
 * This bolt recieves the toll values assesed by the {@link TollNotificationBolt} and answers to account balance
 * queries. Therefore it processes the streams {@link TopologyControl#TOLL_ASSESSMENT_STREAM_ID} (for the former) and
 * {@link TopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID} (for the latter)
 * 
 */
public class AccountBalanceBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AccountBalanceBolt.class);
	
	private OutputCollector collector;
	
	/**
	 * Contains all vehicles and the accountinformation of the current day.
	 */
	private Map<Integer, VehicleAccount> allVehicles;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.allVehicles = new HashMap<Integer, VehicleAccount>();
	}
	
	@Override
	public void execute(Tuple tuple) {
		if(tuple.getSourceStreamId().equals(TopologyControl.TOLL_ASSESSMENTS_STREAM_ID)) {
			this.getBalanceAndSend(tuple);
		} else if(tuple.getSourceStreamId().equals(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID)) {
			this.updateBalance(tuple);
		} else {
			throw new RuntimeException(String.format("Errornous stream subscription. Please report a bug at %s",
				Helper.ISSUE_REPORT_URL));
		}
		
		this.collector.ack(tuple);
	}
	
	private void updateBalance(Tuple tuple) {
		Integer vid = tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME);
		VehicleAccount account = this.allVehicles.get(vid);
		PositionReport pos = (PositionReport)tuple.getValueByField(TopologyControlOld.POS_REPORT_FIELD_NAME);
		if(account == null) {
			int assessedToll = tuple.getIntegerByField(TopologyControl.TOLL_FIELD_NAME);
			account = new VehicleAccount(assessedToll, pos);
			this.allVehicles.put(tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME), account);
		} else {
			// account.assessToll(tuple.getIntegerByField(TopologyControl.TOLL_ASSESSED_FIELD_NAME), pos.getTimer()
			// .getOffset());
		}
	}
	
	private void getBalanceAndSend(Tuple tuple) {
		AccountBalanceRequest bal = (AccountBalanceRequest)tuple
			.getValueByField(TopologyControlOld.ACCOUNT_BALANCE_REQUEST_FIELD_NAME);
		VehicleAccount account = this.allVehicles.get(bal.getVid());
		
		if(account == null) {
			LOG.debug("No account information available yet: at:" + bal.getTime() + " for request" + bal);
			AccountBalance accountBalance = new AccountBalance(bal.getTime(), bal.getQid(), 0, // balance
				0, // tollTime
				bal.getTime());
			this.collector.emit(accountBalance);
		} else {
			AccountBalance accountBalance = account.getAccBalanceNotification(bal);
			this.collector.emit(accountBalance);
		}
	}
	
	public Map<Integer, VehicleAccount> getAllVehicles() {
		return this.allVehicles;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TopologyControlOld.BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME));
	}
	
}
