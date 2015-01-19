package de.hub.cs.dbis.aeolus.testUtils;

/*
 * #%L
 * testUtils
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

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
 * {@link IncSpout} emit tuples with random values to one or multiple output streams. The output schema has a single
 * integer attribute with name {@code a} by default. However, up to 10 attributes can be generated (with names {@code a}
 * to {@code j}). The generated numbers for each attribute are between {@code 1} and {@link #maxValue} (both inclusive).
 * 
 * @author Matthias J. Sax
 */
// TODO add acking/failing support
public class RandomSpout implements IRichSpout {
	private static final long serialVersionUID = -2903431146131196173L;
	
	private final Logger logger = LoggerFactory.getLogger(RandomSpout.class);
	
	private final static String[] attributes = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
	
	private final int numberOfAttributes;
	private final int maxValue;
	private final Object[] values;
	private final String[] outputStreams;
	private final Random r;
	
	private SpoutOutputCollector collector;
	
	
	
	/**
	 * Instantiates a new {@link RandomSpout} that emits tuple with a single attribute to the default output stream.
	 * 
	 * @param maxValue
	 *            The maximum value of the generated numbers (inclusive).
	 */
	public RandomSpout(int maxValue) {
		this(1, maxValue, new String[] {Utils.DEFAULT_STREAM_ID}, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link RandomSpout} that emits tuple with a single attribute to the given output streams.
	 * 
	 * @param maxValue
	 *            The maximum value of the generated numbers (inclusive).
	 * @param outputStreams
	 *            The IDs of the output stream to use.
	 */
	public RandomSpout(int maxValue, String[] outputStreams) {
		this(1, maxValue, outputStreams, System.currentTimeMillis());
	}
	
/**
	 * Instantiates a new {@link RandomSpout} that emits tuple with {@code numberOfAttributes attributes to the
	 * default output stream.
	 * 
	 * @param numberOfAttributes
	 *            The number of attributes of the output tuples.
	 * @param maxValue
	 *            The maximum value of the generated numbers (inclusive).
	 */
	public RandomSpout(int numberOfAttributes, int maxValue) {
		this(numberOfAttributes, maxValue, new String[] {Utils.DEFAULT_STREAM_ID}, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link RandomSpout} that emits tuple with {@code numberOfAttributes} attributes to the given
	 * output streams.
	 * 
	 * @param maxValue
	 *            The maximum value of the generated numbers (inclusive).
	 * @param maxValue
	 *            The maximum value of the generated numbers (inclusive).
	 * @param outputStreams
	 *            The IDs of the output stream to use.
	 */
	public RandomSpout(int numberOfAttributes, int maxValue, String[] outputStreams) {
		this(numberOfAttributes, maxValue, outputStreams, System.currentTimeMillis());
	}
	
	/**
	 * Instantiates a new {@link RandomSpout} with initial {@code seed} that emits tuple with {@code numberOfAttributes}
	 * attributes to the given output streams.
	 * 
	 * @param maxValue
	 *            The maximum value of the generated numbers (inclusive).
	 * @param maxValue
	 *            The maximum value of the generated numbers (inclusive).
	 * @param outputStreams
	 *            The IDs of the output stream to use.
	 * @param seed
	 *            Initial seed for the random generator.
	 */
	public RandomSpout(int numberOfAttributes, int maxValue, String[] outputStreams, long seed) {
		assert (numberOfAttributes > 0 && numberOfAttributes <= attributes.length);
		assert (maxValue > 0);
		assert (outputStreams != null);
		assert (outputStreams.length > 0);
		
		this.logger.debug("seed: {}", new Long(seed));
		
		this.numberOfAttributes = numberOfAttributes;
		this.maxValue = maxValue;
		this.values = new Object[numberOfAttributes];
		this.outputStreams = Arrays.copyOf(outputStreams, outputStreams.length);
		
		this.r = new Random(seed);
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") SpoutOutputCollector collector) {
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
		for(int i = 0; i < this.numberOfAttributes; ++i) {
			this.values[i] = new Integer(1 + this.r.nextInt(this.maxValue));
		}
		Values tuple = new Values(this.values);
		
		for(String stream : this.outputStreams) {
			List<Integer> receiverIds = this.collector.emit(stream, tuple);
			this.logger.trace("emitted tuple {} to output stream {} to receiver tasks with IDs {}", tuple, stream,
				receiverIds);
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
			declarer.declareStream(stream, new Fields(Arrays.copyOf(attributes, this.numberOfAttributes)));
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
