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
package de.hub.cs.dbis.aeolus.utils;

import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * {@link TimestampMerger} merges all incoming streams (all physical substreams from all tasks) over all logical
 * producers in ascending timestamp order. The timestamp attribute must be at the same index in all incoming streams, or
 * must have the same attribute name. Input tuples must be in ascending timestamp order within each incoming substream.
 * The timestamp attribute is expected to be of type {@link Long}.
 * 
 * @author Matthias J. Sax
 */
public class TimestampMerger implements IRichBolt {
	private final static long serialVersionUID = -6930627449574381467L;
	private final static Logger logger = LoggerFactory.getLogger(TimestampMerger.class);
	
	/** The original bolt that consumers a stream of input tuples that are ordered by their timestamp attribute. */
	private final IRichBolt wrappedBolt;
	
	/** The index of the timestamp attribute ({@code -1} if attribute name is used). */
	private final int tsIndex;
	
	/** The name of the timestamp attribute ({@code null} if attribute index is used). */
	private final String tsAttributeName;
	
	/** Input tuple buffer for merging. */
	private StreamMerger<Tuple> merger;
	
	
	
	/**
	 * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
	 * 
	 * @param wrappedBolt
	 *            The bolt to be wrapped.
	 * @param tsIndex
	 *            The index of the timestamp attribute.
	 */
	public TimestampMerger(IRichBolt wrappedBolt, int tsIndex) {
		assert (wrappedBolt != null);
		assert (tsIndex >= 0);
		
		logger.debug("Initialize with timestamp index {}", new Integer(tsIndex));
		
		this.wrappedBolt = wrappedBolt;
		this.tsIndex = tsIndex;
		this.tsAttributeName = null;
	}
	
	/**
	 * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
	 * 
	 * @param wrappedBolt
	 *            The bolt to be wrapped.
	 * @param tsAttributeName
	 *            The name of the timestamp attribute.
	 */
	public TimestampMerger(IRichBolt wrappedBolt, String tsAttributeName) {
		assert (wrappedBolt != null);
		assert (tsAttributeName != null);
		
		logger.debug("Initialize with timestamp attribute {}", tsAttributeName);
		
		this.wrappedBolt = wrappedBolt;
		this.tsIndex = -1;
		this.tsAttributeName = tsAttributeName;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// for each logical input stream (ie, each producer bolt), we get an input partition for each of its tasks
		LinkedList<Integer> taskIds = new LinkedList<Integer>();
		for(Entry<GlobalStreamId, Grouping> inputStream : arg1.getThisSources().entrySet()) {
			taskIds.addAll(arg1.getComponentTasks(inputStream.getKey().get_componentId()));
		}
		
		logger.debug("Detected producer tasks: {}", taskIds);
		
		if(this.tsIndex != -1) {
			assert (this.tsAttributeName == null);
			this.merger = new StreamMerger<Tuple>(taskIds, this.tsIndex);
		} else {
			assert (this.tsAttributeName != null);
			this.merger = new StreamMerger<Tuple>(taskIds, this.tsAttributeName);
			
		}
		
		this.wrappedBolt.prepare(arg0, arg1, arg2);
	}
	
	@Override
	public void execute(Tuple tuple) {
		logger.trace("Adding tuple to internal buffer tuple: {}", tuple);
		this.merger.addTuple(new Integer(tuple.getSourceTask()), tuple);
		
		Tuple t;
		while((t = this.merger.getNextTuple()) != null) {
			logger.trace("Extrated tuple from internal buffer for processing: {}", tuple);
			this.wrappedBolt.execute(t);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		this.wrappedBolt.declareOutputFields(arg0);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedBolt.getComponentConfiguration();
	}
	
	@Override
	public void cleanup() {
		this.wrappedBolt.cleanup();
	}
	
}
