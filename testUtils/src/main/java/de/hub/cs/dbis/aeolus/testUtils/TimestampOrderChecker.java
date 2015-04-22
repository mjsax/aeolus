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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * {@link TimestampOrderChecker} is a wrapper for bolts that checks if incoming tuples are ordered by their timestamp
 * attribute. If an out-of-order tuple is detected, a log error message is generated.
 * 
 * @author Matthias J. Sax
 */
public class TimestampOrderChecker implements IRichBolt {
	private final static long serialVersionUID = -6266187713977343965L;
	
	private final static Logger logger = LoggerFactory.getLogger(TimestampOrderChecker.class);
	
	
	
	/**
	 * The bolt to be wrapped.
	 */
	private final IRichBolt wrappedBolt;
	/**
	 * The index of the timestamp attribute ({@code -1} if attribute name is used).
	 */
	private final int tsIndex;
	/**
	 * The name of the timestamp attribute ({@code null} if attribute index is used).
	 */
	private final String tsAttributeName;
	/**
	 * Indicates if timestamp must be strict monotonic (ie, no timestamp duplicates) or not.
	 */
	private final boolean duplicates;
	/**
	 * The timestamp of the previously processed tuple.
	 */
	private long lastTimestamp = Long.MIN_VALUE;
	
	/**
	 * Creates a new {@link TimestampOrderChecker} wrapper that wraps the given bolt instance.
	 * 
	 * @param wrappedBolt
	 *            The bolt to be wrapped.
	 * @param tsIndex
	 *            The index of the timestamp attribute.
	 * @param duplicates
	 *            {@code true} is timestamp duplicates are allowed; {@code false} otherwise.
	 */
	public TimestampOrderChecker(IRichBolt wrappedBolt, int tsIndex, boolean duplicates) {
		assert tsIndex >= 0;
		
		this.wrappedBolt = wrappedBolt;
		this.tsIndex = tsIndex;
		this.tsAttributeName = null;
		this.duplicates = duplicates;
	}
	
	/**
	 * Creates a new {@link TimestampOrderChecker} wrapper that wraps the given bolt instance.
	 * 
	 * @param wrappedBolt
	 *            The bolt to be wrapped.
	 * @param tsAttributeName
	 *            The name of the timestamp attribute.
	 * @param duplicates
	 *            {@code true} is timestamp duplicates are allowed; {@code false} otherwise.
	 */
	public TimestampOrderChecker(IRichBolt wrappedBolt, String tsAttributeName, boolean duplicates) {
		assert this.tsIndex >= 0;
		
		this.wrappedBolt = wrappedBolt;
		this.tsIndex = -1;
		this.tsAttributeName = tsAttributeName;
		this.duplicates = duplicates;
	}
	
	@Override
	public void cleanup() {
		this.wrappedBolt.cleanup();
	}
	
	/**
	 * Checks if the provided tuple has a larger timestamp than the previous tuple. If not, a log message in generated.
	 * In any case, the wrapped bolt's execute method is called with the given input tuple.
	 * 
	 * @param tuple
	 *            tuple to be processed
	 */
	@SuppressWarnings("boxing")
	@Override
	public void execute(Tuple tuple) {
		long currentTimestamp;
		if(this.tsIndex != -1) {
			currentTimestamp = tuple.getLong(this.tsIndex);
		} else {
			currentTimestamp = tuple.getLongByField(this.tsAttributeName);
		}
		
		if(currentTimestamp < this.lastTimestamp || !this.duplicates && currentTimestamp == this.lastTimestamp) {
			logger.error("Out-of-order tuples detected; previous ts {}; current ts {}", new Long(this.lastTimestamp),
				new Long(currentTimestamp));
		}
		this.lastTimestamp = currentTimestamp;
		
		this.wrappedBolt.execute(tuple);
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.wrappedBolt.prepare(arg0, arg1, arg2);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		this.wrappedBolt.declareOutputFields(arg0);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedBolt.getComponentConfiguration();
	}
	
}
