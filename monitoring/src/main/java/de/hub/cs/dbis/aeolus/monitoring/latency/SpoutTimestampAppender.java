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

import java.util.List;

import backtype.storm.spout.SpoutOutputCollector;





/**
 * {@link SpoutTimestampAppender} wraps a spout output collector to append create timestamps to all output tuples. The
 * appended timestamps are taken by {@link System#currentTimeMillis()}.
 * 
 * @author mjsax
 */
class SpoutTimestampAppender extends SpoutOutputCollector {
	/** The original Storm provided collector. */
	private final SpoutOutputCollector collector;
	
	
	
	/**
	 * Instantiates a new {@link SpoutTimestampAppender}.
	 * 
	 * @param collector
	 *            the original Storm provided collector
	 */
	public SpoutTimestampAppender(SpoutOutputCollector collector) {
		super(collector);
		this.collector = collector;
	}
	
	
	
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		if(!streamId.startsWith("aeolus::")) {
			tuple.add(new Long(System.currentTimeMillis()));
		}
		return this.collector.emit(streamId, tuple, messageId);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		if(!streamId.startsWith("aeolus::")) {
			tuple.add(new Long(System.currentTimeMillis()));
		}
		this.collector.emitDirect(taskId, streamId, tuple, messageId);
	}
	
}
