package de.hub.cs.dbis.aeolus.batching;

/*
 * #%L
 * batching
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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;





/**
 * {@link SpoutOutputBatcher} wraps a spout, buffers the spout's output tuples, and emits those tuples in batches. All
 * receiving bolts must be wrapped by an {@link InputDebatcher}.<br />
 * <br />
 * <strong>CAUTION:</strong>The task IDs returned by {@code .emit(...)} of the used {@link SpoutOutputCollector} reflect
 * the task IDs the tuple <em>will</em> be sent to. The tuples might still be in the output buffer and not transfered
 * yet.
 * 
 * @author Matthias J. Sax
 */
public class SpoutOutputBatcher implements IRichSpout {
	private static final long serialVersionUID = -8627934412821417370L;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(SpoutOutputBatcher.class);
	
	/**
	 * The used {@link SpoutBatchCollector} that wraps the actual {@link SpoutOutputCollector}.
	 */
	private SpoutBatchCollector batchCollector;
	/**
	 * The spout that is wrapped.
	 */
	private final IRichSpout wrappedSpout;
	/**
	 * The size of the output batches to be sent.
	 */
	private final int batchSize;
	
	
	
	/**
	 * TODO
	 * 
	 * @param spout
	 * @param batchSize
	 */
	public SpoutOutputBatcher(IRichSpout spout, int batchSize) {
		LOGGER.debug("batchSize: {}", batchSize);
		this.wrappedSpout = spout;
		this.batchSize = batchSize;
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.batchCollector = new SpoutBatchCollector(context, collector, this.batchSize);
		this.wrappedSpout.open(conf, context, this.batchCollector);
	}
	
	@Override
	public void close() {
		// TODO: flush partial batches
		this.wrappedSpout.close();
	}
	
	@Override
	public void activate() {
		this.wrappedSpout.activate();
	}
	
	@Override
	public void deactivate() {
		// TODO: flush partial batches
		this.wrappedSpout.deactivate();
	}
	
	@Override
	public void nextTuple() {
		this.wrappedSpout.nextTuple();
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
		this.wrappedSpout.declareOutputFields(declarer);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedSpout.getComponentConfiguration();
	}
}
