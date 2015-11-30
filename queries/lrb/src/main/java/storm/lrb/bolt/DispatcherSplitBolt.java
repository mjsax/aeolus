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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.TopologyControl;
import storm.lrb.model.AccountBalanceRequest;
import storm.lrb.model.DailyExpenditureRequest;
import storm.lrb.model.TravelTimeRequest;
import storm.lrb.tools.StopWatch;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.lrb.types.AbstractLRBTuple;
import de.hub.cs.dbis.lrb.types.PositionReport;





/**
 * 
 * This Bolt reduces the workload of the spout by taking over Tuple generation and disptching to the appropiate stream
 * as opposed to the disptacher bolt. emits positionreports per xway.
 * 
 */
public class DispatcherSplitBolt extends BaseRichBolt {
	
	/**
     *
     */
	private static final long serialVersionUID = 1L;
	
	private static final Logger LOG = LoggerFactory.getLogger(DispatcherSplitBolt.class);
	
	private int tupleCnt = 0;
	private OutputCollector collector;
	private StopWatch timer = null;
	private volatile boolean firstrun = true;
	
	public DispatcherSplitBolt() {
		this.timer = new StopWatch();
	}
	
	// TODO evtl buffered wschreiben
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
		this.collector = outputCollector;
		
	}
	
	@Override
	public void execute(Tuple tuple) {
		
		this.splitAndEmit(tuple);
		
		this.collector.ack(tuple);
	}
	
	private void splitAndEmit(Tuple tuple) {
		
		AbstractLRBTuple line = (AbstractLRBTuple)tuple.getValueByField(TopologyControl.TUPLE_FIELD_NAME);
		if(this.firstrun) {
			this.firstrun = false;
			this.timer = (StopWatch)tuple.getValueByField(TopologyControl.TIMER_FIELD_NAME);
			LOG.info("Set timer: " + this.timer);
		}
		
		try {
			
			switch(line.getType()) {
			case AbstractLRBTuple.position_report:
				PositionReport pos = (PositionReport)line;
				
				if(this.tupleCnt <= 10) {
					LOG.debug(String.format("Created: %s", pos));
				}
				this.collector.emit(TopologyControl.POSITION_REPORTS_STREAM, tuple, pos);
				this.tupleCnt++;
				break;
			case 2:
				AccountBalanceRequest acc = (AccountBalanceRequest)line;
				this.collector.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, tuple, new Values(acc.getVid(),
					acc));
				break;
			case 3:
				DailyExpenditureRequest exp = (DailyExpenditureRequest)line;
				this.collector.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, tuple, new Values(
					exp.getVid(), exp));
				break;
			case 4:
				TravelTimeRequest est = (TravelTimeRequest)line;
				this.collector
					.emit(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, tuple, new Values(est.getVid(), est));
				break;
			default:
				LOG.debug("Tupel does not match required LRB format" + line);
				
			}
			
		} catch(NumberFormatException e) {
			LOG.error(String.format("Error in line '%s'", line));
			throw new RuntimeException(e);
		} catch(IllegalArgumentException e) {
			LOG.error(String.format("Error in line '%s'", line));
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		
		outputFieldsDeclarer.declareStream(TopologyControl.POSITION_REPORTS_STREAM, new Fields(
			TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.POS_REPORT_FIELD_NAME));
		
		outputFieldsDeclarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, new Fields(
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.ACCOUNT_BALANCE_REQUEST_FIELD_NAME));
		outputFieldsDeclarer.declareStream(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, new Fields(
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME));
		outputFieldsDeclarer.declareStream(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, new Fields(
			TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.TRAVEL_TIME_REQUEST_FIELD_NAME));
	}
	
	@Override
	public void cleanup() {
		super.cleanup();
	}
}
