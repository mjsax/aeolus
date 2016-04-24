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



/**
 * {@link SpoutReportingThread} reports the statistics, collected by a {@link ThroughputCounterSpoutOutputCollector}.
 * 
 * @author Matthias J. Sax
 */
class SpoutReportingThread extends AbstractReportingThread {
	
	/** The internally used {@link ThroughputCounterSpoutOutputCollector}. */
	private final ThroughputCounterSpoutOutputCollector collector;
	
	
	
	/**
	 * Instantiates a new {@link SpoutReportingThread}.
	 * 
	 * @param collector
	 *            The {@link ThroughputCounterSpoutOutputCollector} that collects and reports statistics.
	 * @param interval
	 *            The reporting interval in milliseconds.
	 */
	public SpoutReportingThread(ThroughputCounterSpoutOutputCollector collector, long interval) {
		super(interval);
		this.collector = collector;
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * Reporting is do via {@link ThroughputCounterSpoutOutputCollector#reportCount(long, double)}.
	 */
	@Override
	void doReport(long reportTimestamp, double factor) {
		this.collector.reportCount(reportTimestamp, factor);
	}
	
}
