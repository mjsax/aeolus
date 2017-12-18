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
package de.hub.cs.dbis.aeolus.monitoring.microbenchmarks;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;





public class MicroSpout implements IRichSpout {
	private static final long serialVersionUID = -5423879264958995343L;
	
	final int recordSize;
	final byte[][] records = new byte[100000][];
	
	Random r;
	SpoutOutputCollector collector;
	
	public MicroSpout(final int recordSize) {
		this.recordSize = recordSize;
	}
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		r = new Random(System.currentTimeMillis());
		for(int i = 0; i < records.length; ++i) {
			records[i] = new byte[recordSize];
			r.nextBytes(records[i]);
		}
		
		this.collector = collector;
	}
	
	@Override
	public void close() {}
	
	@Override
	public void activate() {}
	
	@Override
	public void deactivate() {}
	
	@Override
	public void nextTuple() {
		this.collector.emit(new Values(this.records[r.nextInt(records.length)]));
	}
	
	@Override
	public void ack(Object msgId) {}
	
	@Override
	public void fail(Object msgId) {}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
