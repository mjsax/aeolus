/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.hub.cs.dbis.aeolus.queries.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

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
 * 
 * @author Matthias J. Sax
 */
public class TimestampMerger implements IRichBolt {
	private static final long serialVersionUID = -6930627449574381467L;
	
	
	
	/**
	 * The bolt to be wrapped.
	 */
	private final IRichBolt wrappedBolt;
	/**
	 * The index of the timestamp attribute ({@code -1} if attribute name is used).
	 */
	private final int tsIndex;
	/**
	 * The name of the timestamp attribute ({@code null} if attribute index is used).
	 */
	private final String tsAttributeName;
	/**
	 * Input tuple buffer for merging. Contains a list of input tuples for each producer task.
	 */
	private final HashMap<Integer, LinkedList<Tuple>> mergeBuffer = new HashMap<Integer, LinkedList<Tuple>>();
	/**
	 * Maximum timestamp value that was forwarded already;
	 */
	private long latestTs = Long.MIN_VALUE;
	
	
	
	/**
	 * Creates a new TimestampOrderChecker wrapper that wraps the given bolt instance.
	 * 
	 * @param wrappedBolt
	 *            The bolt to be wrapped.
	 * @param tsIndex
	 *            The index of the timestamp attribute.
	 */
	public TimestampMerger(IRichBolt wrappedBolt, int tsIndex) {
		this.wrappedBolt = wrappedBolt;
		this.tsIndex = tsIndex;
		this.tsAttributeName = null;
	}
	
	/**
	 * Creates a new TimestampOrderChecker wrapper that wraps the given bolt instance.
	 * 
	 * @param wrappedBolt
	 *            The bolt to be wrapped.
	 * @param tsAttributeName
	 *            The name of the timestamp attribute.
	 */
	public TimestampMerger(IRichBolt wrappedBolt, String tsAttributeName) {
		this.wrappedBolt = wrappedBolt;
		this.tsIndex = -1;
		this.tsAttributeName = tsAttributeName;
	}
	
	@Override
	public void cleanup() {
		this.wrappedBolt.cleanup();
	}
	
	/**
	 * 
	 * 
	 * @param Next
	 *            tuple to be processed.
	 */
	@Override
	public void execute(Tuple tuple) {
		// if tuple has a ts-value that is equal to the latest ts-value, we can immediately forward the tuple
		if(this.getTsValue(tuple) == this.latestTs) {
			this.wrappedBolt.execute(tuple);
			return;
		}
		
		// add input tuple to merge buffer
		Integer taskId = new Integer(tuple.getSourceTask());
		try {
			this.mergeBuffer.get(taskId).addLast(tuple);
		} catch(NullPointerException e) {
			System.out.println(e);
		}
		
		// check if we can safely extract an input tuple from the buffer and hand it to the wrapped bolt
		while(true) {
			long minTsFound = Long.MAX_VALUE;
			
			for(Entry<Integer, LinkedList<Tuple>> taskBuffer : this.mergeBuffer.entrySet()) {
				try {
					Tuple t = taskBuffer.getValue().getFirst();
					long ts = this.getTsValue(t);
					if(ts < minTsFound) {
						minTsFound = ts;
					}
				} catch(NoSuchElementException e) {
					// if this exception occurs, at least one input buffer is empty;
					// thus, we cannot extract any input tuple from the buffer safely
					// => we are done and return
					return;
				}
			}
			this.latestTs = minTsFound;
			
			// we extract tuples from the task buffer containing the minimum timestamp
			// if multiple tuple which have minimum timestamp are present, we extract all of them
			for(Entry<Integer, LinkedList<Tuple>> taskBuffer : this.mergeBuffer.entrySet()) {
				LinkedList<Tuple> buffer = taskBuffer.getValue();
				while(buffer.size() > 0) {
					Tuple t = buffer.getFirst();
					
					if(this.getTsValue(t) == minTsFound) {
						buffer.removeFirst();
						this.wrappedBolt.execute(t);
					} else {
						// if the timestamp is not equal the minimum timestamp, we are done
						// all remaining tuples will have a larger timestamp than minimum timestamp, too
						break;
					}
				}
				
			}
		}
	}
	
	private long getTsValue(Tuple tuple) {
		if(this.tsIndex != -1) {
			return tuple.getLong(this.tsIndex).longValue();
		}
		
		return tuple.getLongByField(this.tsAttributeName).longValue();
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// we need to get an entry in mergeBuffer for each task we receive tuples from in order to merge tuples
		// correctly
		// for each logical input stream, we need to get all tasks
		for(Entry<GlobalStreamId, Grouping> inputStream : arg1.getThisSources().entrySet()) {
			// get ID of Bolt that write to the input stream
			String producer = inputStream.getKey().get_componentId();
			
			// get all tasks ID of the producer bolt and add an entry to the buffer for each task
			for(Integer task : arg1.getComponentTasks(producer)) {
				this.mergeBuffer.put(task, new LinkedList<Tuple>());
			}
		}
		
		// prepare original7wrapped bolt...
		this.wrappedBolt.prepare(arg0, arg1, arg2);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		this.wrappedBolt.declareOutputFields(arg0);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedBolt.getComponentConfiguration();
	}
	
}
