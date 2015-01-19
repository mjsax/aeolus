package storm.lrb.bolt;

/*
 * #%L
 * lrb
 * $Id:$
 * $HeadURL:$
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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

import org.apache.log4j.Logger;

import storm.lrb.model.AccBalRequest;
import storm.lrb.model.DaiExpRequest;
import storm.lrb.model.PosReport;
import storm.lrb.model.TTEstRequest;
import storm.lrb.tools.StopWatch;


/**
 * 
 * This Bolt reduces the workload of the spout by taking over Tuple generation
 * and disptching to the appropiate stream
 * 
 */
public class DispatcherBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = Logger.getLogger(DispatcherBolt.class);
	int tupleCnt = 0;
	private OutputCollector _collector;
	private StopWatch timer= new StopWatch();

	private int processed_xway;
	private Long offset=0L;
	
	private volatile boolean firstrun = true;



	// TODO evtl buffered wschreiben
	@Override
	public void prepare(Map conf, TopologyContext topologyContext,
			OutputCollector outputCollector) {
		_collector = outputCollector;
		
	}

	@Override
	public void execute(Tuple tuple) {
	
		splitAndEmit(tuple);
		
		_collector.ack(tuple);
	}

	private void splitAndEmit(Tuple tuple) {
		
		
		String line = tuple.getString(0);
		if(firstrun){
			firstrun = false;
			timer = (StopWatch) tuple.getValue(1);
			LOG.info("Set timer: "+timer);
		}
		String tmp = line.substring(0,1);
		if(!tmp.matches("^[0-4]")) return;
		
		try {
			
			switch (Integer.parseInt(tmp)) {
			case 0:
				PosReport pos = new PosReport(line, timer);
				
				_collector.emit( "PosReports",tuple,
						new Values(pos.getXway(),pos.getDir(), pos.getXD(), pos.getXsd(),pos.getVid(),  pos));
				//_collector.emit( "PosReports",
					//	new Values(pos.getXway(),pos.getDir(), pos.getXD(), pos.getXsd(),pos.getVid(),  pos));
				
				break;
			case 2:
				AccBalRequest acc = new AccBalRequest(line, timer);
				_collector.emit("AccBalRequests", tuple, new Values(acc.getVid(),acc));
				//_collector.emit("AccBalRequests", new Values(acc.getVid(),acc));
				break;
			case 3:
				DaiExpRequest exp = new DaiExpRequest(line, timer);
				_collector.emit("DaiExpRequests",tuple,new Values(exp.getVid(),  exp));
				//_collector.emit("DaiExpRequests",new Values(exp.getVid(),  exp));
				break;
			case 4:
				TTEstRequest est = new TTEstRequest(line, timer);
				_collector.emit("TTEstRequests", tuple, new Values(est.getVid(),est));
				break;
			default:
				//System.out.println("Ignore tuple");
				LOG.debug("Tupel does not match required LRB format" + line);
				
			}
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
			System.out.println("Fehler"+line);
		}catch(IllegalArgumentException e){
			System.out.println("Fehler"+line);
			e.printStackTrace();
		}

	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

		outputFieldsDeclarer.declareStream("PosReports", new Fields("xway", "dir", "xd","xsd", "vid", "PosReport"));

		// declarer.declareStream("PosReports", new Fields("xway", "xsd", "vid",
		// "PosReport"));

		outputFieldsDeclarer.declareStream("AccBalRequests", new Fields("vid","AccBalRequests"));
		outputFieldsDeclarer.declareStream("DaiExpRequests", new Fields("vid","DaiExpRequests"));
		outputFieldsDeclarer.declareStream("TTEstRequests", new Fields("vid","TTEstRequests"));
	}

	

	@Override
	public void cleanup() {
		
		super.cleanup();

	}
}