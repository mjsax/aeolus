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
package de.hub.cs.dbis.aeolus.queries.utils;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;





/**
 * {@link DataDrivenStreamRateDriverSpout} wraps a working spout (with high output rate) and assures a
 * <em>data driven</em> output data rate. Hence, each tuple emitted by the wrapped spout is expected to have a timestamp
 * attribute. {@link DataDrivenStreamRateDriverSpout} ensures that the produces output rate meets the output rate
 * defined by this timestamp values. Thus, replaying a data stream from file can be done with the original data rate. *
 * The timestamp attribute is expected to be of type {@link Number}.
 * 
 * 
 * @author Matthias J. Sax
 */
// TODO: add support to specify timestamp attribute by name
public class DataDrivenStreamRateDriverSpout<T extends Number> implements IRichSpout {
	private final static long serialVersionUID = -2459573797603618679L;
	
	
	
	/**
	 * The original spout that produces output tuples.
	 */
	private IRichSpout wrappedSpout;
	/**
	 * The index of the timestamp attribute ({@code -1} if attribute name is used).
	 */
	private final int tsIndex;
	/**
	 * The time unit used in the timestamp attribute.
	 */
	private final TimeUnit timeUnit;
	/**
	 * The timestamp of the first emitted tuple
	 */
	private long startTS;
	/**
	 * Wrapper for the original {@link SpoutOutputCollector} that extract the timestamp for each emitted tuple.
	 */
	private DataDrivenStreamRateDriverCollector<T> timestampChecker;
	
	
	
	/**
	 * The time unit of the used timestamp.
	 * 
	 * @author Matthias J. Sax
	 */
	public enum TimeUnit {
		SECONDS(1000 * 1000 * 1000), MICROSECONDS(1000 * 1000), MILLISECONDS(1000), NANOSECONDS(1);
		
		private final long factor;
		
		private TimeUnit(int factor) {
			this.factor = factor;
		}
		
		public long factor() {
			return this.factor;
		}
	}
	
	
	
	/**
	 * Instantiates a new {@link DataDrivenStreamRateDriverSpout} that wraps the given spout.
	 * 
	 * @param spout
	 *            The working spout.
	 * @param tsIndex
	 *            The index of the timestamp attribute.
	 */
	public DataDrivenStreamRateDriverSpout(IRichSpout spout, int tsIndex, TimeUnit timeUnit) {
		assert (spout != null);
		assert (timeUnit != null);
		assert (tsIndex >= 0);
		
		this.wrappedSpout = spout;
		this.tsIndex = tsIndex;
		this.timeUnit = timeUnit;
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.timestampChecker = new DataDrivenStreamRateDriverCollector<T>(collector, this.tsIndex);
		this.wrappedSpout.open(conf, context, this.timestampChecker);
	}
	
	@Override
	public void close() {
		this.wrappedSpout.close();
	}
	
	@Override
	public void activate() {
		this.wrappedSpout.activate();
		this.startTS = System.nanoTime();
	}
	
	@Override
	public void deactivate() {
		this.wrappedSpout.deactivate();
	}
	
	@Override
	public void nextTuple() {
		this.wrappedSpout.nextTuple();
		
		long nextValidEmitTime = this.startTS + this.timestampChecker.timestampLastTuple * this.timeUnit.factor;
		
		while(nextValidEmitTime - System.nanoTime() > 0) {
			// busy wait
		}
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
