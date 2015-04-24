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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;





/**
 * {@link BatchSpoutOutputCollector} is used by {@link SpoutOutputBatcher} to capture all calls to the original provided
 * {@link SpoutOutputCollector}. It used {@link SpoutBatchCollectorImpl} to buffer all emitted tuples in batches.
 * 
 * @author Matthias J. Sax
 */
class BatchSpoutOutputCollector extends SpoutOutputCollector {
	private final static Logger logger = LoggerFactory.getLogger(BatchSpoutOutputCollector.class);
	
	/**
	 * The originally provided collector object.
	 */
	final ISpoutOutputCollector collector;
	/**
	 * The internally used BatchCollector.
	 */
	private final SpoutBatchCollectorImpl batcher;
	/**
	 * Is set to {@code true}, each time any {@code emit(...)} or {@code emitDirect(...)} method of this
	 * {@link BatchSpoutOutputCollector} is called. Needs to be reset to {@code false} externally (see
	 * {@link SpoutOutputBatcher#nextTuple()}.
	 */
	boolean tupleEmitted;
	/**
	 * Is set to {@code true} (by {@link SpoutBatchCollectorImpl}), each time a batch is emitted by {@link #batcher}.
	 * Needs to be reset to {@code false} externally (see {@link SpoutOutputBatcher#nextTuple()}.
	 */
	boolean batchEmitted;
	
	
	
	/**
	 * Instantiates a new {@link BatchSpoutOutputCollector} for the given batch size.
	 * 
	 * @param context
	 *            The current runtime environment.
	 * @param collector
	 *            The original collector object.
	 * @param batchSize
	 *            The batch size to be used for all output streams.
	 */
	public BatchSpoutOutputCollector(TopologyContext context, ISpoutOutputCollector collector, int batchSize) {
		super(collector);
		this.collector = collector;
		this.batcher = new SpoutBatchCollectorImpl(this, context, batchSize);
	}
	
	/**
	 * Instantiates a new {@link BatchSpoutOutputCollector} for the given batch size.
	 * 
	 * @param context
	 *            The current runtime environment.
	 * @param collector
	 *            The original collector object.
	 * @param batchSizes
	 *            The batch sizes for each output stream.
	 */
	public BatchSpoutOutputCollector(TopologyContext context, ISpoutOutputCollector collector,
		Map<String, Integer> batchSizes) {
		super(collector);
		this.collector = collector;
		this.batcher = new SpoutBatchCollectorImpl(this, context, batchSizes);
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 * The given message ID is ignored right now because acking and failing is not yet supported.
	 * 
	 * @return currently {@code null} is returned, because the receiver task IDs cannot be determined if it is only
	 *         inserted into an output batch but not actual emit happens
	 */
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		logger.trace("streamId: {}; tuple: {}; messageId: {}", streamId, tuple, messageId);
		this.tupleEmitted = true;
		return this.batcher.tupleEmit(streamId, null, tuple, messageId);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 * The given message ID is ignored right now because acking and failing is not yet supported.
	 * 
	 * @return currently {@code null} is returned, because the receiver task IDs cannot be determined if it is only
	 *         inserted into an output batch but not actual emit happens
	 */
	// need to override to redirect call to SpoutBatchCollector.emit(String streamId, List<Object> tuple, Object
	// messageId)
	@Override
	public List<Integer> emit(List<Object> tuple, Object messageId) {
		return this.emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 * 
	 * @return currently {@code null} is returned, because the receiver task IDs cannot be determined if it is only
	 *         inserted into an output batch but not actual emit happens
	 */
	// need to override to redirect call to SpoutBatchCollector.emit(List<Object> tuple, Object messageId)
	@Override
	public List<Integer> emit(List<Object> tuple) {
		return this.emit(tuple, null);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 * 
	 * @return currently {@code null} is returned, because the receiver task IDs cannot be determined if it is only
	 *         inserted into an output batch but not actual emit happens
	 */
	// need to override to redirect call to SpoutBatchCollector.emit(String streamId, List<Object> tuple, Object
	// messageId)
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		return this.emit(streamId, tuple, null);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 * The given message ID is ignored right now because acking and failing is not yet supported.
	 */
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		logger.trace("taskId: {}; streamId: {}; tuple: {}; messageId: {}", new Integer(taskId), streamId, tuple,
			messageId);
		this.tupleEmitted = true;
		this.batcher.tupleEmitDirect(taskId, streamId, null, tuple, messageId);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 * The given message ID is ignored right now because acking and failing is not yet supported.
	 */
	// need to override to redirect call to SpoutBatchCollector.emitDirect(int taskId, String streamId, List<Object>
	// tuple, Object messageId)
	@Override
	public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 */
	// need to override to redirect call to SpoutBatchCollector.emitDirect(int taskId, String streamId, List<Object>
	// tuple, Object messageId)
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		this.emitDirect(taskId, streamId, tuple, null);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * The tuple is not emitted directly, but is added to an output batch. Output batches are emitted if they are full.
	 */
	// need to override to redirect call to SpoutBatchCollector.emitDirect(int taskId, String streamId, List<Object>
	// tuple, Object messageId)
	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		this.emitDirect(taskId, tuple, null);
	}
	
	/**
	 * Emits all incomplete batches from the output buffer.
	 */
	public void flush() {
		this.batcher.flush();
	}
	
}
