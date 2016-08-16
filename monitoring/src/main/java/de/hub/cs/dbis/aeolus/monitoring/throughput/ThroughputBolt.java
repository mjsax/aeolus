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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import de.hub.cs.dbis.aeolus.monitoring.MonitoringTopoloyBuilder;





/**
 * {@link ThroughputBolt} counts the number of received and emitted tuples and reports the count in regular intervals.
 * The counts are grouped by stream ID and and overall count is reported, too.<br />
 * <br />
 * Internally, it uses {@link BoltThroughputCounter} for input and output streams.
 * 
 * @author mjsax
 */
public class ThroughputBolt implements IRichBolt {
	private final static long serialVersionUID = -2588575856564324599L;
	private final static Logger logger = LoggerFactory.getLogger(ThroughputBolt.class);
	
	/** The original user bolt. */
	private IRichBolt userBolt;
	
	/** The reporting interval in milliseconds; */
	private long interval;
	
	/** The name of the report stream. */
	private final String reportStream;
	
	/** The counter used to monitor incoming streams. */
	private BoltThroughputCounter inputCounter;
	
	/** A asynchrony reporting thread, to report collected input stream statistics periodically. */
	private BoltInputReportingThread inputReporter;
	
	/** A asynchrony reporting thread, to report collected output stream statistics periodically. */
	private BoltOutputReportingThread outputReporter;
	
	/** Indicated if the monitored bolt is a sink or not. For sink, output stream monitoring and reporting is disabled. */
	private final boolean isSink;
	
	
	
	/**
	 * Instantiates a new {@link ThroughputBolt} that report the throughput of the given (non-sink) bolt to the default
	 * report stream {@link MonitoringTopoloyBuilder#DEFAULT_THROUGHPUT_STREAM}.
	 * 
	 * @param userBolt
	 *            The user bolt to be monitored.
	 * @param interval
	 *            The reporting interval in milliseconds.
	 */
	public ThroughputBolt(IRichBolt userBolt, long interval) {
		this(userBolt, interval, MonitoringTopoloyBuilder.DEFAULT_THROUGHPUT_STREAM, false);
	}
	
	/**
	 * Instantiates a new {@link ThroughputBolt} that report the throughput of the given (non-sink) bolt to the
	 * specified stream.
	 * 
	 * @param userBolt
	 *            The user bolt to be monitored.
	 * @param interval
	 *            The reporting interval in milliseconds.
	 * @param reportStream
	 *            The name of the report stream.
	 */
	public ThroughputBolt(IRichBolt userBolt, long interval, String reportStream) {
		this(userBolt, interval, reportStream, false);
	}
	
	/**
	 * Instantiates a new {@link ThroughputBolt} that report the throughput of the given bolt to the default report
	 * stream {@link MonitoringTopoloyBuilder#DEFAULT_THROUGHPUT_STREAM}. For sinks, output stream reporting is
	 * disabled.
	 * 
	 * @param userBolt
	 *            The user bolt to be monitored.
	 * @param interval
	 *            The reporting interval in milliseconds.
	 * @param isSink
	 *            Indicates if monitored bolt is a sink or not.
	 */
	public ThroughputBolt(IRichBolt userBolt, long interval, boolean isSink) {
		this(userBolt, interval, MonitoringTopoloyBuilder.DEFAULT_THROUGHPUT_STREAM, isSink);
	}
	
	/**
	 * Instantiates a new {@link ThroughputBolt} that report the throughput of the given bolt to the specified stream.
	 * For sinks, output stream reporting is disabled.
	 * 
	 * @param userBolt
	 *            The user bolt to be monitored.
	 * @param interval
	 *            The reporting interval in milliseconds.
	 * @param reportStream
	 *            The name of the report stream.
	 * @param isSink
	 *            Indicates if monitored bolt is a sink or not.
	 */
	public ThroughputBolt(IRichBolt userBolt, long interval, String reportStream, boolean isSink) {
		this.userBolt = userBolt;
		this.interval = interval;
		this.reportStream = reportStream;
		this.isSink = isSink;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		final int taskId = context.getThisTaskId();
		this.inputCounter = new BoltThroughputCounter(collector, this.reportStream, true, taskId);
		this.inputReporter = new BoltInputReportingThread(this.inputCounter, this.interval);
		this.inputReporter.start();
		
		if(!this.isSink) {
			ThroughputOutputCollector col = new ThroughputOutputCollector(collector, this.reportStream, taskId);
			collector = col;
			
			this.outputReporter = new BoltOutputReportingThread(col, this.interval);
			this.outputReporter.start();
		}
		
		this.userBolt.prepare(stormConf, context, collector);
	}
	
	@Override
	public void execute(Tuple input) {
		this.inputCounter.countIn(input.getSourceStreamId());
		this.userBolt.execute(input);
	}
	
	@Override
	public void cleanup() {
		this.inputReporter.isRunning = false;
		try {
			this.inputReporter.join();
		} catch(InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
		
		if(!this.isSink) {
			this.outputReporter.isRunning = false;
			try {
				this.outputReporter.join();
			} catch(InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		this.userBolt.cleanup();
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Additionally to the output streams declared by the monitored user bolt, a statistical output stream is declared.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.userBolt.declareOutputFields(declarer);
		AbstractThroughputCounter.declareStatsStream(this.reportStream, declarer);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.userBolt.getComponentConfiguration();
	}
	
}
