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
 * {@link AbstractReportingThread} triggers the reporting of collected statistics periodically.
 * 
 * @author Matthias J. Sax
 */
abstract class AbstractReportingThread extends Thread {
	/**
	 * Indicates if the thread is still running.
	 */
	boolean isRunning = true;
	/**
	 * The reporting interval in milliseconds.
	 */
	private final long interval;
	/**
	 * The timestamp for the next reporting.
	 */
	private long nextReportTime;
	
	
	
	/**
	 * Instantiates a new reporting thread, that reports statistics each {@link #interval} milliseconds.
	 * 
	 * @param interval
	 *            The reporting interval in milliseconds.
	 */
	public AbstractReportingThread(long interval) {
		this.interval = interval;
	}
	
	
	
	@Override
	public void run() {
		this.nextReportTime = System.currentTimeMillis();
		
		while(this.isRunning) {
			while(System.currentTimeMillis() < this.nextReportTime) {
				// busy wait
			}
			this.doReport(this.nextReportTime);
			this.nextReportTime += this.interval;
		}
	}
	
	/**
	 * Called, each time when the statistic values have to be reported.
	 * 
	 * @param reportTimestamp
	 *            The current reporting timestamp.
	 */
	abstract void doReport(long reportTimestamp);
	
}
