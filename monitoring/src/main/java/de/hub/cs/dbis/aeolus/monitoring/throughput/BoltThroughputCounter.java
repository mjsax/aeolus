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

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;





/**
 * {@link BoltThroughputCounter} reports statistical values to a regular stream in a topology using an
 * {@link OutputCollector}.
 * 
 * @author Matthias J. Sax
 */
class BoltThroughputCounter extends AbstractThroughputCounter {
	
	/** The internally used output collector. */
	private final OutputCollector collector;
	
	/** The ID of the report stream. */
	private final String reportStream;
	
	
	
	/**
	 * Instantiates a new {@link BoltThroughputCounter} that emits the statistical values to the stream
	 * {@code reportStream} using the given output collector.
	 * 
	 * @param collector
	 *            The output collector for emitting statistical values.
	 * @param reportStream
	 *            The ID of the statistical report stream.
	 * @param inputOrOutput
	 *            Indicates if input ({@code true}) or output ({@code false}) streams are monitored.
	 */
	public BoltThroughputCounter(OutputCollector collector, String reportStream, boolean inputOrOutput) {
		super(inputOrOutput);
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
