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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	private static final Logger logger = LoggerFactory.getLogger(VerifyBolt.class);
	
	public static final String SPOUT_ID = "noBatchingSpout";
	public static final String BATCHING_SPOUT_ID = "batchingSpout";
	
	private final Fields tupleSchema;
	private final Fields partitions;
	
	private OutputCollector collector;
	private Integer taskId;
	
	private final Map<Set<Object>, LinkedList<Tuple>> noBatching = new HashMap<Set<Object>, LinkedList<Tuple>>();
	private final Map<Set<Object>, LinkedList<Tuple>> batching = new HashMap<Set<Object>, LinkedList<Tuple>>();
	
	public static List<String> errorMessages = new LinkedList<String>();
	public static int matchedTuples = 0;
	
	
	
	public VerifyBolt(Fields schema, Fields partitions) {
		this.tupleSchema = schema;
		this.partitions = partitions;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		this.collector = collector;
		this.taskId = new Integer(context.getThisTaskId());
	}
	
	
	@Override
	public void execute(Tuple input) {
		logger.trace("received {}: {}", this.taskId, input.getValues());
		
		Set<Object> key = null;
		if(this.partitions != null) {
			key = new HashSet<Object>(input.select(this.partitions));
		}
		LinkedList<Tuple> noBatchingBuffer = this.noBatching.get(key);
		if(noBatchingBuffer == null) {
			noBatchingBuffer = new LinkedList<Tuple>();
			this.noBatching.put(key, noBatchingBuffer);
		}
		LinkedList<Tuple> batchingBuffer = this.batching.get(key);
		if(batchingBuffer == null) {
			batchingBuffer = new LinkedList<Tuple>();
			this.batching.put(key, batchingBuffer);
		}
		
		String spoutId = input.getSourceComponent();
		if(spoutId.equals(SPOUT_ID)) {
			if(batchingBuffer.size() == 0) {
				noBatchingBuffer.add(input);
			} else {
				assert (noBatchingBuffer.size() == 0);
				Tuple t = batchingBuffer.pop();
				if(!input.getValues().equals(t.getValues())) {
					errorMessages.add("received tuple does not match expected one: " + input + " vs. " + t);
					logger.error("received tuple does not match expected one: {} vs. {}", input, t);
				} else {
					++matchedTuples;
				}
			}
		} else {
			assert (spoutId.equals(BATCHING_SPOUT_ID));
			if(noBatchingBuffer.size() == 0) {
				batchingBuffer.add(input);
			} else {
				assert (batchingBuffer.size() == 0);
				Tuple t = noBatchingBuffer.pop();
				if(!input.getValues().equals(t.getValues())) {
					errorMessages.add("received tuple does not match expected one: " + input + " vs. " + t);
					logger.error("received tuple does not match expected one: {} vs. {}", input, t);
				} else {
					++matchedTuples;
				}
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
