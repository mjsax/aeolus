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
package de.hub.cs.dbis.aeolus.monitoring.throughput;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;





/**
 * {@link SpoutThroughputCounter} reports statistical values to a regular stream in a topology using an
 * {@link SpoutOutputCollector}.
 * 
 * @author mjsax
 */
class SpoutThroughputCounter extends AbstractThroughputCounter {
	
	/** The internally used output collector. */
	private final SpoutOutputCollector collector;
	
	/** The ID of the report stream. */
	protected final String reportStream;
	
	
	
	/**
	 * Instantiates a new {@link SpoutThroughputCounter} that emits the statistical values to the stream
	 * {@code reportStream} using the given output collector.
	 * 
	 * @param collector
	 *            The output collector for emitting statistical values.
	 * @param reportStream
	 *            The ID of the statistical report stream.
	 * @param taskId
	 *            The task ID.
	 */
	public SpoutThroughputCounter(SpoutOutputCollector collector, String reportStream, int taskId) {
		super(false, taskId);
		this.collector = collector;
		this.reportStream = reportStream;
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * The report is done, by emitting the value to the configures output stream.
	 */
	@Override
	void doEmit(Values statsTuple) {
		this.collector.emit(this.reportStream, statsTuple);
	}
	
}
