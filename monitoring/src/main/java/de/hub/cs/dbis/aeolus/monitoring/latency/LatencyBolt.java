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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * {@link LatencyBolt} appends a "create timestamp" attribute to all emitted tuples. The appended timestamps are taken
 * from the current input tuple's create timestamp attribute (see {@link SpoutTimestampAppender}).
 * 
 * @author mjsax
 */
public class LatencyBolt implements IRichBolt {
	private static final long serialVersionUID = -4286831058178290593L;
	
	/** The original user bolt. */
	private final IRichBolt userBolt;
	
	/** The collector that forwards the input tuple's "create timestampe". */
	private BoltTimestampAppender collector;
	
	
	
	/**
	 * Instantiate a new {@link LatencyBolt} that appends "create timestamps" to all emitted tuples of the given bolt.
	 * 
	 * @param userBolt
	 *            The user bolt to be monitored.
	 */
	public LatencyBolt(IRichBolt userBolt) {
		this.userBolt = userBolt;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = new BoltTimestampAppender(collector);
		this.userBolt.prepare(stormConf, context, this.collector);
	}
	
	@Override
	public void execute(Tuple input) {
		this.collector.createTimestamp = input.getLong(input.size() - 1);
		this.userBolt.execute(input);
	}
	
	@Override
	public void cleanup() {
		this.userBolt.cleanup();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		TimestampAttributeAppender.addCreateTimestampAttributeToSchemas(this.userBolt, declarer);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.userBolt.getComponentConfiguration();
	}
	
}
