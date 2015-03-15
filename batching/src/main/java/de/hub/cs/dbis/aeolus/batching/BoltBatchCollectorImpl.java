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

import java.util.Collection;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;





/**
 * {@link BoltBatchCollectorImpl} performs back calls to a {@link BoltBatchCollector}.
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
	 * The {@link BoltBatchCollector} that used this instance of an {@link AbstractBatchCollector}.
	 */
	private final BoltBatchCollector boltBatchCollector;
	
	
	
	/**
	 * Instantiates a new {@link BoltBatchCollectorImpl} that back calls the original Storm provided
	 * {@link OutputCollector} in order to emit a {@link Batch} of tuples.
	 * 
	 * @param boltBatchCollector
	 *            The {@link BoltBatchCollector} for call backs.
	 * @param context
	 *            The current runtime environment.
	 * @param batchSize
	 *            The size of the output batches to be built.
	 */
	BoltBatchCollectorImpl(BoltBatchCollector boltBatchCollector, TopologyContext context, int batchSize) {
		super(context, batchSize);
		this.boltBatchCollector = boltBatchCollector;
	}
	
	
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	protected List<Integer> batchEmit(String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
		assert (messageId == null);
		BoltBatchCollector.logger.trace("streamId: {}; anchors: {}, batch: {}", streamId, anchors, batch);
		return this.boltBatchCollector.collector.emit(streamId, anchors, (List)batch);
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	protected void batchEmitDirect(int taskId, String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
		assert (messageId == null);
		BoltBatchCollector.logger.trace("taskId: {}; streamId: {}; anchors: {}, batch: {}", new Integer(taskId),
			streamId, anchors, batch);
		this.boltBatchCollector.collector.emitDirect(taskId, streamId, anchors, (List)batch);
	}
	
}
