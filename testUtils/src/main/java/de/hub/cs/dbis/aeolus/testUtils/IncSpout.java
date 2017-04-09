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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;





/**
 * {@link IncSpout} emits tuples with increasing values to one or multiple output streams (ie, data is replicated to all
 * output streams). The output schema has a single {@link Long} attribute with name {@code id}. The first emitted tuple
 * has value {@code 0}. Additionally, {@link IncSpout} can skip the emit step in regular intervals (by default, skipping
 * is disabled).
 * 
 * @author mjsax
 */
// TODO add acking/failing support
public class IncSpout implements IRichSpout {
	private final static long serialVersionUID = -2903431146131196173L;
	private final static Logger logger = LoggerFactory.getLogger(IncSpout.class);
	
	private final Random r;
	
	private final String[] outputStreams;
	private SpoutOutputCollector collector;
	private long currentValue = 0;
	private final double duplicatesProbability;
	private final int stepSize;
	private final int skipInterval;
	private int counter = 0;
	
	
	/**
	 * Instantiates a new {@link IncSpout} that emits unique values with step size one to the default output stream.
	 */
	public IncSpout() {
		this(new String[] {Utils.DEFAULT_STREAM_ID}, 0.0, 1, 0, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link IncSpout} that emits unique values with step size one to the default output stream.
	 * Each {@code skipInterval} {@link #nextTuple()} call will not emit a tuple. If {@code skipInterval} is smaller
	 * than 2, skipping is disabled.
	 * 
	 * @param skipInterval
	 *            The interval between two {@link #nextTuple()} calls that do not emit.
	 */
	public IncSpout(int skipInterval) {
		this(new String[] {Utils.DEFAULT_STREAM_ID}, 0.0, 1, skipInterval, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link IncSpout} that emits to the default output stream.
	 * 
	 * If unique values should be emitted, {@code probability} should be set to zero (or any negative value). For value
	 * greater or equal to one, all emitted tuples will have the same value.
	 * 
	 * @param probability
	 *            The probability that duplicates occur.
	 * @param stepSize
	 *            The step size for increasing values.
	 */
	public IncSpout(double probability, int stepSize) {
		this(new String[] {Utils.DEFAULT_STREAM_ID}, probability, stepSize, 0, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link IncSpout} that emits unique values to the given output streams.
	 * 
	 * @param outputStreamIds
	 *            The IDs of the output stream to use.
	 */
	public IncSpout(String[] outputStreamIds) {
		this(outputStreamIds, 0, 1, 0, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link IncSpout} that emits to the given output streams with given duplicates probability.
	 * 
	 * If unique values should be emitted, {@code probability} should be set to zero (or any negative value). For value
	 * greater or equal to one, all emitted tuples will have the same value.
	 * 
	 * @param outputStreamIds
	 *            The IDs of the output stream to use.
	 * @param probability
	 *            The probability that duplicates occur.
	 * @param stepSize
	 *            The step size for increasing values.
	 */
	public IncSpout(String[] outputStreamIds, double probability, int stepSize) {
		this(outputStreamIds, probability, stepSize, 0, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link IncSpout} that emits to the given output streams with given duplicates probability.
	 * 
	 * If unique values should be emitted, {@code probability} should be set to zero (or any negative value). For value
	 * greater or equal to one, all emitted tuples will have the same value.
	 * 
	 * @param outputStreamIds
	 *            The IDs of the output stream to use.
	 * @param probability
	 *            The probability that duplicates occur.
	 * @param stepSize
	 *            The step size for increasing values.
	 * @param skipInterval
	 *            The interval between two {@link #nextTuple()} calls that do not emit. If {@code skipInterval} is
	 *            smaller than 2, skipping is disabled.
	 * @param seed
	 *            Initial seed for randomly generating duplicates.
	 */
	public IncSpout(String[] outputStreamIds, double probability, int stepSize, int skipInterval, long seed)
		throws IllegalArgumentException {
		assert (outputStreamIds != null);
		assert (outputStreamIds.length > 0);
		assert (stepSize > 0);
		
		this.outputStreams = Arrays.copyOf(outputStreamIds, outputStreamIds.length);
		this.duplicatesProbability = probability;
		this.stepSize = stepSize;
		this.skipInterval = skipInterval;
		this.r = new Random(seed);
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void close() {
		// nothing to do
	}
	
	@Override
	public void activate() {
		// nothing to do
	}
	
	@Override
	public void deactivate() {
		// nothing to do
	}
	
	@Override
	public void nextTuple() {
		if(this.skipInterval < 2 || ++this.counter % this.skipInterval != 0) {
			Values tuple = new Values(new Long(this.currentValue));
			
			for(String stream : this.outputStreams) {
				List<Integer> receiverIds = this.collector.emit(stream, tuple);
				logger.trace("emitted tuple {} to output stream {} to receiver tasks with IDs {}", tuple, stream,
					receiverIds);
			}
			
			if(this.r.nextDouble() >= this.duplicatesProbability) {
				this.currentValue += this.stepSize;
			}
		} else {
			this.counter = 0;
		}
	}
	
	@Override
	// TODO
	public void ack(Object msgId) {
		throw new UnsupportedOperationException("Not implemented yet.");
	}
	
	@Override
	// TODO
	public void fail(Object msgId) {
		throw new UnsupportedOperationException("Not implemented yet.");
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for(String stream : this.outputStreams) {
			declarer.declareStream(stream, new Fields("id"));
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
