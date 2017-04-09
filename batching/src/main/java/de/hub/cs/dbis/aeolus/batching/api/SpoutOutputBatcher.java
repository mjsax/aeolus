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
package de.hub.cs.dbis.aeolus.batching.api;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import de.hub.cs.dbis.aeolus.batching.AbstractBatchCollector;
import de.hub.cs.dbis.aeolus.batching.BatchSpoutOutputCollector;
import de.hub.cs.dbis.aeolus.batching.BatchingOutputFieldsDeclarer;





/**
 * {@link SpoutOutputBatcher} wraps a spout, buffers the spout's output tuples, and emits those tuples in batches. All
 * receiving bolts must be wrapped by an {@link InputDebatcher}.<br/>
 * <br/>
 * <strong>CAUTION:</strong>Calls to {@code .emit(...)} will return {@code null}, because the tuples might still be in
 * the output buffer and not transfered yet.<br />
 * <br />
 * <strong>CAUTION:</strong>Tuple acking, failing, and anchoring is currently not supported.
 * 
 * @author mjsax
 */
public class SpoutOutputBatcher implements IRichSpout {
	private final static long serialVersionUID = -8627934412821417370L;
	
	/**
	 * The used {@link BatchSpoutOutputCollector} that wraps the actual {@link SpoutOutputCollector}.
	 */
	private BatchSpoutOutputCollector batchCollector;
	/**
	 * The spout that is wrapped.
	 */
	private final IRichSpout wrappedSpout;
	/**
	 * The sizes of the output batches for each output stream.
	 */
	private final Map<String, Integer> batchSizes;
	/**
	 * The size of the output batches (for all output streams).
	 */
	private final int batchSize;
	
	
	
	/**
	 * Instantiates a new {@link SpoutOutputBatcher} the emits the output of the given {@link IRichSpout} in batches of
	 * size {@code batchSize}.
	 * 
	 * @param spout
	 *            The original spout to be wrapped.
	 * @param batchSize
	 *            The batch size to be used for all output streams.
	 * 
	 * @throws IllegalArgumentException
	 *             if {@code spout} is {@code null} or {@code batchSize} is not positive
	 */
	public SpoutOutputBatcher(IRichSpout spout, int batchSize) {
		if(spout == null) {
			throw new IllegalArgumentException("Parameter <spout> must not be null.");
		}
		if(batchSize < 1) {
			throw new IllegalArgumentException("Parameter <batchSize> must not greater than 0.");
		}
		this.wrappedSpout = spout;
		this.batchSizes = null;
		this.batchSize = batchSize;
	}
	
	/**
	 * Instantiates a new {@link SpoutOutputBatcher} the emits the output of the given {@link IRichSpout} in batches of
	 * size {@code batchSize}.
	 * 
	 * @param spout
	 *            The original spout to be wrapped.
	 * @param batchSizes
	 *            The batch sizes for each output stream.
	 * 
	 * @throws IllegalArgumentException
	 *             if {@code spout} or {@code batchSizes} is {@code null}
	 */
	public SpoutOutputBatcher(IRichSpout spout, Map<String, Integer> batchSizes) {
		if(spout == null) {
			throw new IllegalArgumentException("Parameter <spout> must not be null.");
		}
		if(batchSizes == null) {
			throw new IllegalArgumentException("Parameter <batchSizes> must not be null.");
		}
		this.wrappedSpout = spout;
		this.batchSizes = batchSizes;
		this.batchSize = -1;
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		if(this.batchSizes != null) {
			this.batchCollector = new BatchSpoutOutputCollector(context, collector, this.batchSizes);
		} else {
			this.batchCollector = new BatchSpoutOutputCollector(context, collector, this.batchSize);
		}
		this.wrappedSpout.open(conf, context, this.batchCollector);
	}
	
	@Override
	public void close() {
		this.wrappedSpout.close();
		this.batchCollector.flush();
	}
	
	@Override
	public void activate() {
		this.wrappedSpout.activate();
	}
	
	@Override
	public void deactivate() {
		this.wrappedSpout.deactivate();
		this.batchCollector.flush();
	}
	
	@Override
	public void nextTuple() {
		/*
		 * In order to avoid a waiting penalty (because of a missing emit), we try to fill up a complete batch before
		 * returning. If the wrapped spout does not add a new tuple to an output batch we return as well in order to
		 * avoid busy waiting within the while-true-loop.
		 */
		while(true) {
			this.batchCollector.tupleEmitted = false;
			this.batchCollector.batchEmitted = false;
			
			this.wrappedSpout.nextTuple();
			
			if(!this.batchCollector.tupleEmitted || this.batchCollector.batchEmitted) {
				break;
			}
		}
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO: receiving ack for whole batch -> reply acks for single tuples to user spout
		// TODO: store original Ids of all tuples for each batch
		this.wrappedSpout.ack(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		// TODO: receiving fail for whole batch -> reply fails for single tuples to user spout
		// TODO: store original Ids of all tuples for each batch
		this.wrappedSpout.fail(msgId);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.wrappedSpout.declareOutputFields(new BatchingOutputFieldsDeclarer(declarer));
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedSpout.getComponentConfiguration();
	}
	
	/**
	 * Registers internally used classes for serialization and deserialization. Same as
	 * {@link BoltOutputBatcher#registerKryoClasses(Config)} and {@link InputDebatcher#registerKryoClasses(Config)}.
	 * 
	 * @param stormConfig
	 *            The storm config the which the classes should be registered to.
	 */
	public static void registerKryoClasses(Config stormConfig) {
		AbstractBatchCollector.registerKryoClasses(stormConfig);
	}
	
}
