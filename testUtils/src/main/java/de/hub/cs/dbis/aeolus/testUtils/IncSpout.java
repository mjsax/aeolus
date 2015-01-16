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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;





/**
 * {@link IncSpout} emit tuples with increasing values to one or multiple output streams. The output schema has a single
 * integer attribute with name {@code id}. The first emitted tuple as value {@code 1}.
 * 
 * @author Matthias J. Sax
 */
// TODO add acking/failing support
public class IncSpout implements IRichSpout {
	private static final long serialVersionUID = -2903431146131196173L;
	private final Logger logger = LoggerFactory.getLogger(IncSpout.class);
	
	private final String[] outputStreams;
	private SpoutOutputCollector collector;
	private int currentValue = 0;
	
	
	
	/**
	 * Instantiates a new {@link IncSpout} that emits to the default output stream.
	 */
	public IncSpout() {
		this(new String[] {Utils.DEFAULT_STREAM_ID});
	}
	
	/**
	 * Instantiates a new {@link IncSpout} that emits to the given output streams.
	 * 
	 * @param outputStreamIds
	 *            The IDs of the output stream to use.
	 */
	public IncSpout(String[] outputStreamIds) {
		assert (outputStreamIds != null);
		assert (outputStreamIds.length > 0);
		
		this.outputStreams = Arrays.copyOf(outputStreamIds, outputStreamIds.length);
	}
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void close() {
		// nothing to do
	}
	
	@Override
	public void activate() {
		// nothing to do
	}
	
	@Override
	public void deactivate() {
		// nothing to do
	}
	
	@Override
	public void nextTuple() {
		Values tuple = new Values(new Integer(++this.currentValue));
		
		for(String stream : this.outputStreams) {
			List<Integer> receiverIds = this.collector.emit(stream, tuple);
			this.logger.trace("emitted tuple {} to output stream {} to receiver tasks with IDs {}", tuple, stream,
				receiverIds);
		}
	}
	
	@Override
	// TODO
	public void ack(Object msgId) {
		throw new UnsupportedOperationException("Not implemented yet.");
	}
	
	@Override
	// TODO
	public void fail(Object msgId) {
		throw new UnsupportedOperationException("Not implemented yet.");
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for(String stream : this.outputStreams) {
			declarer.declareStream(stream, new Fields("id"));
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
