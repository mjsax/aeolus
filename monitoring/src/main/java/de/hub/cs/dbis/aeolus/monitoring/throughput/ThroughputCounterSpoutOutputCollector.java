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

import java.util.List;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;





/**
 * {@link ThroughputCounterSpoutOutputCollector} wraps a spout output collector to monitor the output streams of a
 * spout.
 * 
 * @author Matthias J. Sax
 */
class ThroughputCounterSpoutOutputCollector extends SpoutOutputCollector {
	
	/** The internally used counter. */
	private SpoutThroughputCounter counter;
	
	
	
	/**
	 * Instantiates a new {@link ThroughputCounterSpoutOutputCollector}.
	 * 
	 * @param collector
	 *            The original output collector.
	 * @param reportStream
	 *            The ID of the report stream.
	 */
	public ThroughputCounterSpoutOutputCollector(SpoutOutputCollector collector, String reportStream) {
		super(collector);
		this.counter = new SpoutThroughputCounter(collector, reportStream);
	}
	
	
	
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		this.counter.countOut(streamId);
		return super.emit(streamId, tuple, messageId);
	}
	
	@Override
	public List<Integer> emit(List<Object> tuple, Object messageId) {
		return this.emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}
	
	@Override
	public List<Integer> emit(List<Object> tuple) {
		return this.emit(tuple, null);
	}
	
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		return this.emit(streamId, tuple, null);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		this.counter.countOut(streamId);
		super.emitDirect(taskId, streamId, tuple, messageId);
	}
	
	@Override
	public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		this.emitDirect(taskId, streamId, tuple, null);
	}
	
	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		this.emitDirect(taskId, tuple, null);
	}
	
	/**
	 * Forwards the reporting request to the internally used counter.
	 * 
	 * @param ts
	 *            The timestamp when this report is triggered.
	 * @param factor
	 *            The normalization factor to get reported values "per second".
	 */
	void reportCount(long ts, double factor) {
		this.counter.reportCount(ts, factor);
	}
	
}
