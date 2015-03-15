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
package de.hub.cs.dbis.aeolus.batching;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;





/**
 * {@link BoltBatchCollector} is used by {@link BoltOutputBatcher} to capture all calls to the original provided
 * {@link OutputCollector}. It used {@link BoltBatchCollectorImpl} to buffer all emitted tuples in batches.
 * 
 * @author Matthias J. Sax
 */
class BoltBatchCollector extends OutputCollector {
	final static Logger logger = LoggerFactory.getLogger(BoltBatchCollector.class);
	
	/**
	 * The originally provided collector object.
	 */
	IOutputCollector collector;
	/**
	 * The internally used BatchCollector.
	 */
	private final BoltBatchCollectorImpl batcher;
	
	
	
	/**
	 * Instantiates a new {@link BoltBatchCollector} for the given batch size.
	 * 
	 * @param context
	 *            The current runtime environment.
	 * @param collector
	 *            The original collector object.
	 * @param batchSize
	 *            The size of the output batches to be built.
	 */
	public BoltBatchCollector(TopologyContext context, IOutputCollector collector, int batchSize) {
		super(collector);
		logger.trace("batchSize: {}", new Integer(batchSize));
		
		this.collector = collector;
		this.batcher = new BoltBatchCollectorImpl(this, context, batchSize);
	}
	
	
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emit(String streamId, Collection<Tuple> anchors,
	// List<Object> tuple)
	@Override
	public List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple) {
		return this.emit(streamId, Arrays.asList(anchor), tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emit(String streamId, Collection<Tuple> anchors,
	// List<Object> tuple)
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		return this.emit(streamId, (Collection<Tuple>)null, tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emit(String streamId, Collection<Tuple> anchors,
	// List<Object> tuple)
	@Override
	public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
		return this.emit(Utils.DEFAULT_STREAM_ID, anchors, tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emit(String streamId, Collection<Tuple> anchors,
	// List<Object> tuple)
	@Override
	public List<Integer> emit(Tuple anchor, List<Object> tuple) {
		return this.emit(Utils.DEFAULT_STREAM_ID, anchor, tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emit(String streamId, List<Object> tuple)
	@Override
	public List<Integer> emit(List<Object> tuple) {
		return this.emit(Utils.DEFAULT_STREAM_ID, tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emitDirect(int taskId, String streamId, Collection<Tuple>
	// anchors, List<Object> tuple)
	@Override
	public void emitDirect(int taskId, String streamId, Tuple anchor, List<Object> tuple) {
		this.emitDirect(taskId, streamId, Arrays.asList(anchor), tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emitDirect(int taskId, String streamId, Collection<Tuple>
	// anchors, List<Object> tuple)
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		this.emitDirect(taskId, streamId, (List<Tuple>)null, tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emitDirect(int taskId, String streamId, Collection<Tuple>
	// anchors, List<Object> tuple)
	@Override
	public void emitDirect(int taskId, Collection<Tuple> anchors, List<Object> tuple) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchors, tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emitDirect(int taskId, String streamId, Collection<Tuple>
	// anchors, List<Object> tuple)
	@Override
	public void emitDirect(int taskId, Tuple anchor, List<Object> tuple) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchor, tuple);
	}
	
	/**
	 * TODO
	 */
	// need to override to redirect call to BoltBatchCollector.emitDirect(int taskId, String streamId, List<Object>
	// tuple)
	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple);
	}
	
	/**
	 * TODO
	 */
	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		logger.trace("streamId: {}; anchors: {}; tuple: {}", streamId, anchors, tuple);
		return this.batcher.tupleEmit(streamId, anchors, tuple, null);
	}
	
	/**
	 * TODO
	 */
	@Override
	public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		logger.trace("taskId: {}; streamId: {}; anchors: {}; tuple: {}", new Integer(taskId), streamId, anchors, tuple);
		this.batcher.tupleEmitDirect(taskId, streamId, anchors, tuple, null);
	}
	
	@Override
	public void ack(Tuple input) {
		this.collector.ack(input);
	}
	
	@Override
	public void fail(Tuple input) {
		this.collector.fail(input);
	}
	
	@Override
	public void reportError(Throwable error) {
		this.collector.reportError(error);
	}
}
