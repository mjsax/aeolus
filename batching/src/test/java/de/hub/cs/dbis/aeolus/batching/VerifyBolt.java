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

import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;





/**
 * @author Matthias J. Sax
 */
public class VerifyBolt implements IRichBolt {
	private static final long serialVersionUID = -2047329782139913124L;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(VerifyBolt.class);
	
	public static final String SPOUT_ID = "noBatchingSpout";
	public static final String BATCHING_SPOUT_ID = "batchingSpout";
	
	private final Fields tupleSchema;
	
	private OutputCollector collector;
	private Integer taskId;
	
	LinkedList<Tuple> noBatching = new LinkedList<Tuple>();
	LinkedList<Tuple> batching = new LinkedList<Tuple>();
	
	
	
	public VerifyBolt(Fields schema) {
		this.tupleSchema = schema;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
		this.taskId = context.getThisTaskId();
	}
	
	
	@Override
	public void execute(Tuple input) {
		LOGGER.debug("received {}: {}", this.taskId, input.getValues());
		
		String spoutId = input.getSourceComponent();
		if(spoutId.equals(SPOUT_ID)) {
			if(this.batching.size() == 0) {
				this.noBatching.add(input);
			} else {
				assert (input.getValues().equals(this.batching.pop().getValues()));
			}
		} else {
			assert (spoutId.equals(BATCHING_SPOUT_ID));
			if(this.noBatching.size() == 0) {
				this.batching.add(input);
			} else {
				assert (input.getValues().equals(this.noBatching.pop().getValues()));
			}
		}
		
		this.collector.ack(input);
	}
	
	@Override
	public void cleanup() {
		// nothing to do
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.tupleSchema);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
