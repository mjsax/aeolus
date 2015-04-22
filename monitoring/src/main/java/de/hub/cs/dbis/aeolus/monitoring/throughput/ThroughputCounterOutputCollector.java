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
package de.hub.cs.dbis.aeolus.monitoring.throughput;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;





/**
 * {@link ThroughputCounterOutputCollector} wraps a output collector to monitor the output streams of a bolt.
 * 
 * @author Matthias J. Sax
 */
class ThroughputCounterOutputCollector extends OutputCollector {
	/**
	 * The internally used counter.
	 */
	private final BoltThroughputCounter counter;
	
	
	
	/**
	 * Instantiates a new {@link ThroughputCounterOutputCollector}.
	 * 
	 * @param collector
	 *            The original output collector.
	 * @param reportStream
	 *            The ID of the report stream.
	 */
	public ThroughputCounterOutputCollector(OutputCollector collector, String reportStream) {
		super(collector);
		this.counter = new BoltThroughputCounter(collector, reportStream, false);
	}
	
	
	
	@Override
	public List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple) {
		return this.emit(streamId, Arrays.asList(anchor), tuple);
	}
	
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		return this.emit(streamId, (List<Tuple>)null, tuple);
	}
	
	@Override
	public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
		return this.emit(Utils.DEFAULT_STREAM_ID, anchors, tuple);
	}
	
	@Override
	public List<Integer> emit(Tuple anchor, List<Object> tuple) {
		return this.emit(Utils.DEFAULT_STREAM_ID, anchor, tuple);
	}
	
	@Override
	public List<Integer> emit(List<Object> tuple) {
		return this.emit(Utils.DEFAULT_STREAM_ID, tuple);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, Tuple anchor, List<Object> tuple) {
		this.emitDirect(taskId, streamId, Arrays.asList(anchor), tuple);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		this.emitDirect(taskId, streamId, (List<Tuple>)null, tuple);
	}
	
	@Override
	public void emitDirect(int taskId, Collection<Tuple> anchors, List<Object> tuple) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchors, tuple);
	}
	
	@Override
	public void emitDirect(int taskId, Tuple anchor, List<Object> tuple) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchor, tuple);
	}
	
	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
	}
	
	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		this.counter.countOut(streamId);
		return super.emit(streamId, anchors, tuple);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		this.counter.countOut(streamId);
		super.emitDirect(taskId, streamId, anchors, tuple);
	}
	
	/**
	 * Forwards the reporting request to the internally used counter.
	 * 
	 * @param ts
	 *            The timestamp when this report is triggered.
	 */
	void reportCount(long ts) {
		this.counter.reportCount(ts);
	}
	
}
