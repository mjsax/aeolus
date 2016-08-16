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

import java.util.Collection;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;





/**
 * {@link BoltTimestampAppender} wraps a bolt output collector to appends create timestamps to all output tuples. The
 * timestamp that should be appended must be provided explicitly by setting {@link #createTimestamp}.
 * 
 * @author mjsax
 */
class BoltTimestampAppender extends OutputCollector {
	/** The original Storm provided collector. */
	private OutputCollector collector;
	
	/** The timestamp that should be appended to output tuples. */
	Long createTimestamp;
	
	/**
	 * Instantiates a new {@link BoltTimestampAppender}.
	 * 
	 * @param collector
	 *            the original Storm provided collector
	 */
	public BoltTimestampAppender(OutputCollector collector) {
		super(collector);
		this.collector = collector;
	}
	
	
	
	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		if(!streamId.startsWith("aeolus::")) {
			tuple.add(this.createTimestamp);
		}
		return this.collector.emit(streamId, anchors, tuple);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		if(!streamId.startsWith("aeolus::")) {
			tuple.add(this.createTimestamp);
		}
		this.collector.emitDirect(taskId, streamId, anchors, tuple);
	}
	
}
