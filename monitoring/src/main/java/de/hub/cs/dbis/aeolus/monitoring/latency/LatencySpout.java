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
package de.hub.cs.dbis.aeolus.monitoring.latency;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;





/**
 * {@link LatencySpout} appends a "create timestamp" attribute to all emitted tuples. The appended timestamps are taken
 * by {@link System#currentTimeMillis()}.
 * 
 * @author mjsax
 */
public class LatencySpout implements IRichSpout {
	private final static long serialVersionUID = -127630826178083995L;
	
	/** The original user spout. */
	private final IRichSpout userSpout;
	
	
	
	/**
	 * Instantiate a new {@link LatencySpout} that appends "create timestamps" to all emitted tuples of the given spout.
	 * 
	 * @param userSpout
	 *            The user spout to be monitored.
	 */
	public LatencySpout(IRichSpout userSpout) {
		this.userSpout = userSpout;
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.userSpout.open(conf, context, new SpoutTimestampAppender(collector));
	}
	
	@Override
	public void close() {
		this.userSpout.close();
	}
	
	@Override
	public void activate() {
		this.userSpout.activate();
	}
	
	@Override
	public void deactivate() {
		this.userSpout.deactivate();
	}
	
	@Override
	public void nextTuple() {
		this.userSpout.nextTuple();
	}
	
	@Override
	public void ack(Object msgId) {
		this.userSpout.ack(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		this.userSpout.fail(msgId);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		TimestampAttributeAppender.addCreateTimestampAttributeToSchemas(this.userSpout, declarer);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.userSpout.getComponentConfiguration();
	}
	
}
