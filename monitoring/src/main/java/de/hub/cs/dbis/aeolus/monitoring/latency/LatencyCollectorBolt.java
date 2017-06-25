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

import java.util.Map;

import org.apache.storm.guava.collect.TreeMultiset;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.monitoring.MonitoringTopoloyBuilder;





/**
 * {@link LatencyCollectorBolt} appends a "create timestamp" attribute to all emitted tuples. The appended timestamps
 * are taken from the current input tuple's create timestamp attribute (see {@link SpoutTimestampAppender}).
 * 
 * @author mjsax
 */
public class LatencyCollectorBolt implements IRichBolt {
	private static final long serialVersionUID = -4286831058178290593L;
	
	/** The original user bolt. */
	private final IRichBolt userBolt;
	
	/** The number of tuples to compute latency statistics. */
	private final int bucketSize;
	
	/** The name of the report stream. */
	private final String reportStream;
	
	/** The original Storm provided collector. */
	private OutputCollector stormCollector;
	/** The collector that provide the "finished processing" timestamp. */
	private BoltEndTimestampCollector collector;
	
	private final TreeMultiset<Long> orderedLatencies = TreeMultiset.create();
	private final Long[] asArray;
	private long sum = 0;
	
	
	
	/**
	 * Instantiate a new {@link LatencyCollectorBolt} that report latency statistics to the default report stream
	 * {@link MonitoringTopoloyBuilder#DEFAULT_LATENCY_STREAM}.
	 * 
	 * @param userBolt
	 *            The user bolt to be monitored.
	 * @param statsBucketSize
	 *            The number of tuples of a statistic bucket.
	 */
	public LatencyCollectorBolt(IRichBolt userBolt, int statsBucketSize) {
		this(userBolt, statsBucketSize, MonitoringTopoloyBuilder.DEFAULT_LATENCY_STREAM);
	}
	
	/**
	 * Instantiate a new {@link LatencyCollectorBolt} that report latency statistics to the specified stream.
	 * 
	 * @param userBolt
	 *            The user bolt to be monitored.
	 * @param statsBucketSize
	 *            The number of tuples of a statistic bucket.
	 * @param reportStream
	 *            The name of the report stream.
	 */
	public LatencyCollectorBolt(IRichBolt userBolt, int statsBucketSize, String reportStream) {
		this.userBolt = userBolt;
		this.bucketSize = statsBucketSize;
		this.asArray = new Long[this.bucketSize];
		this.reportStream = reportStream;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.stormCollector = collector;
		this.collector = new BoltEndTimestampCollector(collector);
		this.userBolt.prepare(stormConf, context, this.collector);
	}
	
	@Override
	public void execute(Tuple input) {
		long createTimestamp = input.getLong(input.size() - 1).longValue();
		this.userBolt.execute(input);
		
		long latency = this.collector.endTimestamp - createTimestamp;
		this.sum += latency;
		this.orderedLatencies.add(new Long(latency));
		if(this.orderedLatencies.size() == this.bucketSize) {
			this.orderedLatencies.toArray(this.asArray);
			
			Long max = this.asArray[this.bucketSize - 1];
			Long percentil99 = this.asArray[(this.bucketSize * 99) / 100 - 1];
			Long percentil95 = this.asArray[(this.bucketSize * 95) / 100 - 1];
			Long median = this.asArray[this.bucketSize / 2 - 1];
			Long avg = new Long(this.sum / this.bucketSize);
			Long min = this.asArray[0];
			
			this.stormCollector.emit(this.reportStream, new Values(new Long(System.currentTimeMillis()), max,
				percentil99, percentil95, median, avg, min));
			
			this.orderedLatencies.clear();
			this.sum = 0;
		}
	}
	
	@Override
	public void cleanup() {
		this.userBolt.cleanup();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.userBolt.declareOutputFields(declarer);
		declarer.declareStream(this.reportStream, new Fields("ts", "max", "99", "95", "med", "avg", "min"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.userBolt.getComponentConfiguration();
	}
	
}
