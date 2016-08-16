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
package de.hub.cs.dbis.aeolus.monitoring;

import java.io.File;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import de.hub.cs.dbis.aeolus.monitoring.latency.LatencyBolt;
import de.hub.cs.dbis.aeolus.monitoring.latency.LatencyCollectorBolt;
import de.hub.cs.dbis.aeolus.monitoring.latency.LatencySpout;
import de.hub.cs.dbis.aeolus.monitoring.throughput.ThroughputBolt;
import de.hub.cs.dbis.aeolus.monitoring.throughput.ThroughputSpout;
import de.hub.cs.dbis.aeolus.sinks.FileFlushSinkBolt;





/**
 * {@link MonitoringTopoloyBuilder} allows to automatically insert monitoring wrapper spouts and bolt into a topology to
 * collect throughput and latency statistics.
 * 
 * @author mjsax
 */
public class MonitoringTopoloyBuilder extends TopologyBuilder {
	/** The default ID of the throughput report stream. */
	public final static String DEFAULT_THROUGHPUT_STREAM = "aeolus::throughput";
	/** The default ID of the latency report stream. */
	public final static String DEFAULT_LATANCY_STREAM = "aeolus::latency";
	/** The default directory to write monitoring statistics. */
	public final static String DEFAULT_STATS_DIR = "/tmp/aeolus-stats";
	
	private final boolean meassureThroughput;
	private final int reportingInterval;
	private final boolean meassureLatency;
	private final int statsBucketSize;
	
	private boolean callSuper = false;
	
	
	
	public MonitoringTopoloyBuilder(boolean meassureThroughput, int reportingInterval, boolean meassureLatency,
		int statsBucketSize) {
		this.meassureThroughput = meassureThroughput;
		this.reportingInterval = reportingInterval;
		this.meassureLatency = meassureLatency;
		this.statsBucketSize = statsBucketSize;
	}
	
	
	
	@Override
	public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
		if(this.meassureThroughput) {
			spout = new ThroughputSpout(spout, this.reportingInterval);
		}
		if(this.meassureLatency) {
			spout = new LatencySpout(spout);
		}
		
		SpoutDeclarer declarer = super.setSpout(id, spout, parallelismHint);
		
		if(this.meassureThroughput) {
			this.callSuper = true;
			setBolt(id + "Stats", new FileFlushSinkBolt(DEFAULT_STATS_DIR + File.separator + id + ".throughput"))
				.shuffleGrouping(id, MonitoringTopoloyBuilder.DEFAULT_THROUGHPUT_STREAM);
			this.callSuper = false;
		}
		
		return declarer;
	}
	
	@Override
	public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
		if(callSuper) {
			return super.setBolt(id, bolt, parallelismHint);
		}
		
		if(this.meassureThroughput) {
			bolt = new ThroughputBolt(bolt, this.reportingInterval);
		}
		if(this.meassureLatency) {
			bolt = new LatencyBolt(bolt);
		}
		
		BoltDeclarer declarer = super.setBolt(id, bolt, parallelismHint);
		
		if(this.meassureThroughput) {
			this.callSuper = true;
			setBolt(id + "Stats", new FileFlushSinkBolt(DEFAULT_STATS_DIR + File.separator + id + ".throughput"))
				.shuffleGrouping(id, MonitoringTopoloyBuilder.DEFAULT_THROUGHPUT_STREAM);
			this.callSuper = false;
		}
		
		return declarer;
	}
	
	public BoltDeclarer setSink(String id, IRichBolt bolt) {
		return this.setSink(id, bolt, null);
	}
	
	public BoltDeclarer setSink(String id, IRichBolt bolt, Number parallelismHint) {
		if(this.meassureThroughput) {
			bolt = new ThroughputBolt(bolt, this.reportingInterval, true);
		}
		if(this.meassureLatency) {
			bolt = new LatencyCollectorBolt(bolt, this.statsBucketSize);
		}
		
		final BoltDeclarer declarer = super.setBolt(id, bolt, parallelismHint);
		
		if(this.meassureLatency) {
			this.callSuper = true;
			setBolt(id + "LatencyStats", new FileFlushSinkBolt(DEFAULT_STATS_DIR + File.separator + id + ".latencies"))
				.shuffleGrouping(id, MonitoringTopoloyBuilder.DEFAULT_LATANCY_STREAM);
			this.callSuper = false;
		}
		
		return declarer;
	}
	
}
