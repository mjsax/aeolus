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

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;





/**
 * {@link BoltEndTimestampCollector} wraps a bolt output collector to collect the "finished processing" timestamp, i.e.,
 * the timestamp when {@link #ack(Tuple)} or {@link #fail(Tuple)} is called for the input tuple. It is assumed that if
 * multiple tuples get acked/failed within a single call to {@link IRichBolt#execute(Tuple)}, the last tuple that gets
 * acked/failed is the original input tuple.
 * 
 * @author mjsax
 */
class BoltEndTimestampCollector extends OutputCollector {
	/** The original Storm provided collector. */
	private OutputCollector collector;
	
	/** The timestamp when a tuple was completely processed (ie, when ack() or fail() was called). */
	long endTimestamp;
	
	/**
	 * Instantiates a new {@link BoltEndTimestampCollector}.
	 * 
	 * @param collector
	 *            the original Storm provided collector
	 */
	public BoltEndTimestampCollector(OutputCollector collector) {
		super(collector);
		this.collector = collector;
	}
	
	
	
	@Override
	public void ack(Tuple input) {
		this.collector.ack(input);
		this.endTimestamp = System.currentTimeMillis();
	}
	
	@Override
	public void fail(Tuple input) {
		this.collector.fail(input);
		this.endTimestamp = System.currentTimeMillis();
	}
	
}
