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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;





/**
 * {@link SpoutBatchCollectorImpl} performs back calls to a {@link SpoutBatchCollector}.
 * 
 * This design is necessary, because multiple inheritance in not supported in Java. Furthermore, the actual logic of
 * output batching is the same for Spouts and Bolts, but both use different interfaces. Thus,
 * {@link AbstractBatchCollector} contains the actual logic, while {@link SpoutBatchCollectorImpl} and
 * {@link BoltBatchCollectorImpl} are used to redirect the back calls appropriately to a Spout or Bolt, respectively.
 * 
 * @author Matthias J. Sax
 */
class SpoutBatchCollectorImpl extends AbstractBatchCollector {
	/**
	 * The {@link SpoutBatchCollector} that used this instance of an {@link AbstractBatchCollector}.
	 */
	private final SpoutBatchCollector spoutBatchCollector;
	
	
	
	/**
	 * Instantiates a new {@link SpoutBatchCollectorImpl} that back calls the original Storm provided
	 * {@link SpoutOutputCollector} in order to emit a {@link Batch} of tuples.
	 * 
	 * @param spoutBatchCollector
	 *            The {@link SpoutBatchCollector} for call backs.
	 * @param context
	 *            The current runtime environment.
	 * @param batchSize
	 *            The size of the output batches to be built.
	 */
	SpoutBatchCollectorImpl(SpoutBatchCollector spoutBatchCollector, TopologyContext context, int batchSize) {
		super(context, batchSize);
		this.spoutBatchCollector = spoutBatchCollector;
	}
	
	
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	protected List<Integer> batchEmit(String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
		assert (anchors == null);
		logger.trace("streamId: {}; batch: {}; messageId: {}", streamId, batch, messageId);
		this.spoutBatchCollector.batchEmitted = true;
		return this.spoutBatchCollector.collector.emit(streamId, (List)batch, messageId);
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	protected void batchEmitDirect(int taskId, String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
		assert (anchors == null);
		logger.trace("taskId: {}; streamId: {}; batch: {}; messageId: {}", new Integer(taskId), streamId, batch,
			messageId);
		this.spoutBatchCollector.batchEmitted = true;
		this.spoutBatchCollector.collector.emitDirect(taskId, streamId, (List)batch, messageId);
	}
	
}
