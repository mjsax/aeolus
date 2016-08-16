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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import de.hub.cs.dbis.aeolus.monitoring.MonitoringTopoloyBuilder;





/**
 * {@link ThroughputSpout} counts the number of emitted tuples and reports the count in regular intervals. The counts
 * are grouped by stream ID and and overall count is reported, too.<br />
 * <br />
 * Internally, it uses a {@link SpoutThroughputCounter}.
 * 
 * @author mjsax
 */
public class ThroughputSpout implements IRichSpout {
	private final static long serialVersionUID = 7077749897674933208L;
	private final static Logger logger = LoggerFactory.getLogger(ThroughputSpout.class);
	
	/** The original user spout. */
	private IRichSpout userSpout;
	
	/** The reporting interval in milliseconds; */
	private long reportingIntervalMs;
	
	/** The name of the report stream. */
	private final String reportStream;
	
	/** A asynchrony reporting thread, to report collected output stream statistics periodically. */
	private SpoutReportingThread reporter;
	
	
	
	/**
	 * Instantiates a new {@link ThroughputSpout} that report the throughput of the given spout to the default report
	 * stream {@link MonitoringTopoloyBuilder#DEFAULT_THROUGHPUT_STREAM}.
	 * 
	 * @param userSpout
	 *            The user spout to be monitored.
	 * @param reportingIntervalMs
	 *            The reporting interval in milliseconds.
	 */
	public ThroughputSpout(IRichSpout userSpout, long reportingIntervalMs) {
		this(userSpout, reportingIntervalMs, MonitoringTopoloyBuilder.DEFAULT_THROUGHPUT_STREAM);
	}
	
	/**
	 * Instantiates a new {@link ThroughputSpout} that report the throughput of the given spout to the specified stream
	 * 
	 * @param userSpout
	 *            The user spout to be monitored.
	 * @param reportingIntervalMs
	 *            The reporting interval in milliseconds.
	 * @param reportStream
	 *            The name of the report stream.
	 */
	public ThroughputSpout(IRichSpout userSpout, long reportingIntervalMs, String reportStream) {
		this.userSpout = userSpout;
		this.reportingIntervalMs = reportingIntervalMs;
		this.reportStream = reportStream;
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		ThroughputSpoutOutputCollector col = new ThroughputSpoutOutputCollector(collector, this.reportStream,
			context.getThisTaskId());
		
		this.reporter = new SpoutReportingThread(col, this.reportingIntervalMs);
		this.reporter.start();
		
		this.userSpout.open(conf, context, col);
	}
	
	@Override
	public void close() {
		this.reporter.isRunning = false;
		try {
			this.reporter.join();
		} catch(InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
		this.userSpout.close();
	}
	
	@Override
	public void activate() {
		this.userSpout.activate();
	}
	
	@Override
	public void deactivate() {
		this.userSpout.deactivate();
	}
	
	@Override
	public void nextTuple() {
		this.userSpout.nextTuple();
	}
	
	@Override
	public void ack(Object msgId) {
		this.userSpout.ack(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		this.userSpout.fail(msgId);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Additionally to the output streams declared by the monitored user spout, a statistical output stream is declared.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.userSpout.declareOutputFields(declarer);
		AbstractThroughputCounter.declareStatsStream(this.reportStream, declarer);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.userSpout.getComponentConfiguration();
	}
	
}
