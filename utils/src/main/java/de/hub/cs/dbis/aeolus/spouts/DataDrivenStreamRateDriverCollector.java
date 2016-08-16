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
package de.hub.cs.dbis.aeolus.spouts;

import java.util.List;

import backtype.storm.spout.SpoutOutputCollector;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;





/**
 * {@link DataDrivenStreamRateDriverCollector} forward all calls to the wrapped {@link SpoutOutputCollector}. Before
 * forwarding, it extract the timestamp attribute value from the currently emitted tuple. The timestamp attribute is
 * expected to be of type {@link Number} and is stored as {@code long}.
 * 
 * @author mjsax
 */
public class DataDrivenStreamRateDriverCollector<T extends Number> extends SpoutOutputCollector {
	
	/** The original output collector. */
	private final SpoutOutputCollector collector;
	
	/** The index of the timestamp attribute. */
	private final int tsIndex;
	
	/** The timestamp of the last emitted tuple. */
	long timestampLastTuple;
	
	
	
	/**
	 * Instantiates a new {@link DataDrivenStreamRateDriverCollector} for the given timestamp attribute index.
	 * 
	 * @param collector
	 *            The collector to be wrapped
	 * @param tsIndex
	 *            The index of the timestamp attribute.
	 */
	public DataDrivenStreamRateDriverCollector(SpoutOutputCollector collector, int tsIndex) {
		super(collector);
		assert (collector != null);
		assert (tsIndex >= 0);
		
		this.collector = collector;
		this.tsIndex = tsIndex;
	}
	
	
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		if(!TimestampMerger.FLUSH_STREAM_ID.equals(streamId)) {
			this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		}
		return this.collector.emit(streamId, tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(List<Object> tuple, Object messageId) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		return this.collector.emit(tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(List<Object> tuple) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		return this.collector.emit(tuple);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		if(!TimestampMerger.FLUSH_STREAM_ID.equals(streamId)) {
			this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		}
		return this.collector.emit(streamId, tuple);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		if(!TimestampMerger.FLUSH_STREAM_ID.equals(streamId)) {
			this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		}
		this.collector.emitDirect(taskId, streamId, tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		this.collector.emitDirect(taskId, tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		if(!TimestampMerger.FLUSH_STREAM_ID.equals(streamId)) {
			this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		}
		this.collector.emitDirect(taskId, streamId, tuple);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		this.collector.emitDirect(taskId, tuple);
	}
	
	@Override
	public void reportError(Throwable error) {
		this.collector.reportError(error);
	}
	
}
