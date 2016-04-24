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
package de.hub.cs.dbis.aeolus.batching;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;





/**
 * {@link BoltBatchCollectorImpl} performs back calls to a {@link BatchOutputCollector}.
 * 
 * This design is necessary, because multiple inheritance in not supported in Java. Furthermore, the actual logic of
 * output batching is the same for Spouts and Bolts, but both use different interfaces. Thus,
 * {@link AbstractBatchCollector} contains the actual logic, while {@link SpoutBatchCollectorImpl} and
 * {@link BoltBatchCollectorImpl} are used to redirect the back calls appropriately to a Spout or Bolt, respectively.
 * 
 * @author Matthias J. Sax
 */
class BoltBatchCollectorImpl extends AbstractBatchCollector {
	/**
	 * The {@link BatchOutputCollector} that used this instance of an {@link AbstractBatchCollector}.
	 */
	private final BatchOutputCollector boltBatchCollector;
	
	
	
	/**
	 * Instantiates a new {@link BoltBatchCollectorImpl} that back calls the original Storm provided
	 * {@link OutputCollector} in order to emit a {@link Batch} of tuples.
	 * 
	 * @param boltBatchCollector
	 *            The {@link BatchOutputCollector} for call backs.
	 * @param context
	 *            The current runtime environment.
	 * @param batchSize
	 *            The batch size to be used for all output streams.
	 */
	public BoltBatchCollectorImpl(BatchOutputCollector boltBatchCollector, TopologyContext context, int batchSize) {
		super(context, batchSize);
		this.boltBatchCollector = boltBatchCollector;
	}
	
	/**
	 * Instantiates a new {@link BoltBatchCollectorImpl} that back calls the original Storm provided
	 * {@link OutputCollector} in order to emit a {@link Batch} of tuples.
	 * 
	 * @param boltBatchCollector
	 *            The {@link BatchOutputCollector} for call backs.
	 * @param context
	 *            The current runtime environment.
	 * @param batchSizes
	 *            The batch sizes for each output stream.
	 */
	public BoltBatchCollectorImpl(BatchOutputCollector boltBatchCollector, TopologyContext context,
		Map<String, Integer> batchSizes) {
		super(context, batchSizes);
		this.boltBatchCollector = boltBatchCollector;
	}
	
	
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	protected List<Integer> doEmit(String streamId, Collection<Tuple> anchors, Object tupleOrBatch, Object messageId) {
		assert (messageId == null);
		BatchOutputCollector.logger
			.trace("streamId: {}; anchors: {}, tuple/batch: {}", streamId, anchors, tupleOrBatch);
		return this.boltBatchCollector.collector.emit(streamId, anchors, (List)tupleOrBatch);
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	protected void doEmitDirect(int taskId, String streamId, Collection<Tuple> anchors, Object tupleOrBatch, Object messageId) {
		assert (messageId == null);
		BatchOutputCollector.logger.trace("taskId: {}; streamId: {}; anchors: {}, tuple/batch: {}",
			new Integer(taskId), streamId, anchors, tupleOrBatch);
		this.boltBatchCollector.collector.emitDirect(taskId, streamId, anchors, (List)tupleOrBatch);
	}
	
}
