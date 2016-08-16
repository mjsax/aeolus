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
package de.hub.cs.dbis.lrb.operators;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;
import de.hub.cs.dbis.lrb.queries.utils.TopologyControl;
import de.hub.cs.dbis.lrb.types.AbstractLRBTuple;
import de.hub.cs.dbis.lrb.types.AccountBalanceRequest;
import de.hub.cs.dbis.lrb.types.DailyExpenditureRequest;
import de.hub.cs.dbis.lrb.types.PositionReport;
import de.hub.cs.dbis.lrb.types.TravelTimeRequest;





/**
 * {@link DispatcherBolt} retrieves a stream of {@code <ts,string>} tuples, parses the second CSV attribute and emits an
 * appropriate LRB tuple. The LRB input CSV schema is:
 * {@code Type, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos, QID, S_init, S_end, DOW, TOD, Day}<br />
 * <br />
 * <strong>Output schema:</strong>
 * <ul>
 * <li>{@link PositionReport} (stream: {@link TopologyControl#POSITION_REPORTS_STREAM_ID})</li>
 * <li>{@link AccountBalanceRequest} (stream: {@link TopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID})</li>
 * <li>{@link DailyExpenditureRequest} (stream: {@link TopologyControl#DAILY_EXPEDITURE_REQUESTS_STREAM_ID})</li>
 * <li>{@link TravelTimeRequest} (stream: {@link TopologyControl#TRAVEL_TIME_REQUEST_STREAM_ID})</li>
 * </ul>
 * 
 * @author mjsax
 **/
public class DispatcherBolt extends BaseRichBolt {
	private static final long serialVersionUID = 6908631355830501961L;
	private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherBolt.class);
	
	/** The storm provided output collector. */
	private OutputCollector collector;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
	}
	
	@Override
	public void execute(Tuple input) {
		if(input.getSourceStreamId().equals(TimestampMerger.FLUSH_STREAM_ID)) {
			this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values((Object)null));
			this.collector.ack(input);
			return;
		}
		
		String raw = null;
		try {
			raw = input.getString(1);
			String[] token = raw.split(",");
			
			// common attributes of all input tuples
			short type = Short.parseShort(token[0]);
			Short time = new Short(input.getLong(0).shortValue());
			Integer vid = new Integer(Integer.parseInt(token[2]));
			
			assert (time.shortValue() == Short.parseShort(token[1]));
			
			if(type == AbstractLRBTuple.position_report) {
				this.collector.emit(TopologyControl.POSITION_REPORTS_STREAM_ID, new PositionReport(//
					time,//
					vid,//
					new Integer(Integer.parseInt(token[3])), // speed
					new Integer(Integer.parseInt(token[4])), // xway
					new Short(Short.parseShort(token[5])), // lane
					new Short(Short.parseShort(token[6])), // direction
					new Short(Short.parseShort(token[7])), // segment
					new Integer(Integer.parseInt(token[8])))); // position
			} else {
				// common attribute of all requests
				Integer qid = new Integer(Integer.parseInt(token[9]));
				
				switch(type) {
				case AbstractLRBTuple.account_balance_request:
					this.collector.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, new AccountBalanceRequest(
						time, vid, qid));
					break;
				case AbstractLRBTuple.daily_expenditure_request:
					this.collector.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
						new DailyExpenditureRequest(time, vid,//
							new Integer(Integer.parseInt(token[4])), // xway
							qid,//
							new Short(Short.parseShort(token[14])))); // day
					break;
				case AbstractLRBTuple.travel_time_request:
					this.collector.emit(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, new TravelTimeRequest(time, vid,//
						new Integer(Integer.parseInt(token[4])), // xway
						qid,//
						new Short(Short.parseShort(token[10])), // S_init
						new Short(Short.parseShort(token[11])), // S_end
						new Short(Short.parseShort(token[12])), // DOW
						new Short(Short.parseShort(token[13])))); // TOD
					break;
				default:
					LOGGER.error("Unkown tuple type: {}", new Short(type));
				}
			}
		} catch(Exception e) {
			LOGGER.error("Error in line: {}", raw);
			LOGGER.error("StackTrace:", e);
		}
		
		this.collector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declareStream(TopologyControl.POSITION_REPORTS_STREAM_ID, PositionReport.getSchema());
		outputFieldsDeclarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
			AccountBalanceRequest.getSchema());
		outputFieldsDeclarer.declareStream(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
			DailyExpenditureRequest.getSchema());
		outputFieldsDeclarer
			.declareStream(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, TravelTimeRequest.getSchema());
		outputFieldsDeclarer.declareStream(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
	}
	
}
