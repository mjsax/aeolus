/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.hub.cs.dbis.aeolus.queries.utils;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;





/**
 * {@link SpoutStreamRateDriver} wraps a working spout (with high output rate) and assures a stable (lower) output data
 * rate. In order to simulate that the working spout is working, busy wait strategy is used.
 * 
 * @author Matthias J. Sax
 */
public class SpoutStreamRateDriver implements IRichSpout {
	private static final long serialVersionUID = 5846769281188227304L;
	
	private IRichSpout wrappedSpout;
	private final long delay; // in ns
	private long nextTS;
	
	
	
	/**
	 * Instantiates a new {@link SpoutStreamRateDriver} for the given spout and output rate.
	 * 
	 * @param spout
	 *            The working spout.
	 * @param outputRate
	 *            The output rate in tuples per second.
	 */
	public SpoutStreamRateDriver(IRichSpout spout, double outputRate) {
		this.wrappedSpout = spout;
		this.delay = (long)((1000 * 1000 * 1000) / outputRate);
	}
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.wrappedSpout.open(conf, context, collector);
	}
	
	@Override
	public void close() {
		this.wrappedSpout.close();
	}
	
	@Override
	public void activate() {
		this.wrappedSpout.activate();
		this.nextTS = System.nanoTime();
	}
	
	@Override
	public void deactivate() {
		this.wrappedSpout.deactivate();
	}
	
	@Override
	public void nextTuple() {
		while(System.nanoTime() < this.nextTS) {
			// busy wait
		}
		this.wrappedSpout.nextTuple();
		this.nextTS += this.delay;
	}
	
	@Override
	public void ack(Object msgId) {
		this.wrappedSpout.ack(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		this.wrappedSpout.fail(msgId);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.wrappedSpout.declareOutputFields(declarer);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedSpout.getComponentConfiguration();
	}
	
}
