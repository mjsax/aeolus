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



/**
 * {@link BoltInputReportingThread} reports the statistics, collected by a {@link BoltThroughputCounter}.
 * 
 * @author Matthias J. Sax
 */
class BoltInputReportingThread extends AbstractReportingThread {
	/**
	 * The internally used {@link BoltThroughputCounter}.
	 */
	private final BoltThroughputCounter counter;
	
	
	
	/**
	 * Instantiates a new {@link BoltInputReportingThread}.
	 * 
	 * @param counter
	 *            The {@link BoltThroughputCounter} that collects and reports statistics.
	 * @param interval
	 *            The reporting interval in milliseconds.
	 */
	public BoltInputReportingThread(BoltThroughputCounter counter, long interval) {
		super(interval);
		this.counter = counter;
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * Reporting is do via {@link BoltThroughputCounter#reportCount(long)}.
	 */
	@Override
	void doReport(long reportTimestamp) {
		this.counter.reportCount(reportTimestamp);
	}
	
}
