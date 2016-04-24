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
package de.hub.cs.dbis.aeolus.monitoring.microbenchmarks;

import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;





/**
 * TODO
 * 
 * @author Matthias J. Sax
 */
public class SchemaSpout implements IRichSpout {
	private final static long serialVersionUID = 7071724037607577082L;
	private final static Logger logger = LoggerFactory.getLogger(SchemaSpout.class);
	
	/** TODO */
	private final Random r;
	
	/** TODO */
	private SpoutOutputCollector collector;
	
	
	
	/**
	 * TODO
	 */
	public SchemaSpout() {
		long seed = System.currentTimeMillis();
		this.r = new Random(seed);
		
		logger.debug("seed: {}", new Long(seed));
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void nextTuple() {
		// byte[] b = new byte[100];
		// this.r.nextBytes(b);
		// this.collector.emit(new Values(b));
		this.collector.emit(new Values(new Integer(this.r.nextInt())));
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("dummy"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
