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
package de.hub.cs.dbis.aeolus.batching;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * TODO
 * 
 * @author Matthias J. Sax
 */
public class BoltOutputBatcher implements IRichBolt {
	private static final long serialVersionUID = 6453060658895879104L;
	
	private final Logger logger = LoggerFactory.getLogger(BoltOutputBatcher.class);
	
	/**
	 * TODO
	 */
	private BoltBatchCollector batchCollector;
	/**
	 * TOOD
	 */
	private final IRichBolt wrappedBolt;
	/**
	 * TODO
	 */
	private final int batchSize;
	
	
	
	/**
	 * TODO
	 * 
	 * @param bolt
	 * @param batchSize
	 */
	BoltOutputBatcher(IRichBolt bolt, int batchSize) {
		this.logger.debug("batchSize: {}", new Integer(batchSize));
		this.wrappedBolt = bolt;
		this.batchSize = batchSize;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.batchCollector = new BoltBatchCollector(context, collector, this.batchSize);
		this.wrappedBolt.prepare(stormConf, context, this.batchCollector);
		
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
