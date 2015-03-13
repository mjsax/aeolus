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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;





/**
 * {@link AbstractBatchCollector} buffers emitted tuples in batches and emits full batches. It is used by
 * {@link SpoutBatchCollector} and {@link BoltBatchCollector}.
 * 
 * {@link AbstractBatchCollector} uses {@code de.hub.cs.dbis.aeolus.batching.StormConnector} which is provided as
 * jar-file. This jar file need to be build manually (see folder aeolus/aeolus-storm-connector).
 * 
 * 
 * @author Matthias J. Sax
 */
// TODO: what about batches of different sizes (for different output streams? or for different consumers?)
abstract class AbstractBatchCollector {
	protected final static Logger LOGGER = LoggerFactory.getLogger(AbstractBatchCollector.class);
	
	/**
	 * The size of the output batches.
	 */
	private final int batchSize;
	/**
	 * The current runtime environment.
	 */
	private final TopologyContext topologyContext;
	/**
	 * The ID of the producer operator which output is buffered by this {@link AbstractBatchCollector}.
	 */
	private final String componentId;
	/**
	 * Maps output streams to their receivers.
	 */
	private final Map<String, List<String>> receivers = new HashMap<String, List<String>>(9);
	/**
	 * Maps receiver components to their tasks.
	 */
	private final Map<String, List<Integer>> receiverTasks = new HashMap<String, List<Integer>>();
	/**
	 * Maps receiver components to their fieldsGrouping attributes.
	 */
	private final Map<String, Fields> groupFields = new HashMap<String, Fields>();
	/**
	 * Maps output streams to receiver tasks to corresponding output buffers.
	 */
	// TODO: right now, we get an output buffer for each receiver task -> fully transparent
	// we could trade "full transparency" for less latency and use single output buffer in some cases (-> no
	// fieldsGrouping) [remember: for each output stream, a single receiver can only subscribe to the stream a single
	// time]
	private final Map<String, Map<Integer, Batch>> outputBuffers = new HashMap<String, Map<Integer, Batch>>();
	
	
	
	/**
	 * Creates a new {@link AbstractBatchCollector} that emits batches of size {@code batchSize}.
	 * 
	 * @param context
	 *            The current runtime environment.
	 * @param batchSize
	 *            The size of the output batches to be built.
	 */
	AbstractBatchCollector(TopologyContext context, int batchSize) {
		LOGGER.trace("batchSize: {}", batchSize);
		
		this.batchSize = batchSize;
		this.topologyContext = context;
		this.componentId = context.getThisComponentId();
		
		// StreamId -> ComponentId -> Grouping
		for(Entry<String, Map<String, Grouping>> outputStream : context.getThisTargets().entrySet()) {
			final String streamId = outputStream.getKey();
			final Map<String, Grouping> streamReceivers = outputStream.getValue();
			
			final int numberOfAttributes = context.getComponentOutputFields(this.componentId, streamId).size();
			
			final ArrayList<String> receiverIds = new ArrayList<String>(streamReceivers.size());
			this.receivers.put(streamId, receiverIds);
			final HashMap<Integer, Batch> taskBuffers = new HashMap<Integer, Batch>();
			this.outputBuffers.put(streamId, taskBuffers);
			
			for(Entry<String, Grouping> receiver : streamReceivers.entrySet()) {
				final String receiverId = receiver.getKey();
				receiverIds.add(receiverId);
				final List<Integer> taskIds = context.getComponentTasks(receiverId);
				this.receiverTasks.put(receiverId, taskIds);
				if(receiver.getValue().is_set_fields()) {
					this.groupFields.put(receiverId, new Fields(receiver.getValue().get_fields()));
				}
				for(Integer taskId : taskIds) {
					taskBuffers.put(taskId, new Batch(batchSize, numberOfAttributes));
				}
				
			}
			
		}
		
	}
	
	/**
	 * Captures an regular emit call of an operator, adds the output tuple to the corresponding output buffers, and
	 * emits all buffers that get filled completely during this call.
	 * 
	 * @param streamId
	 * @param anchors
	 * @param tuple
	 * @param messageId
	 * 
	 * @return
	 */
	public List<Integer> tupleEmit(String streamId, Collection<Tuple> anchors, List<Object> tuple, Object messageId) {
		final List<Integer> computedTaskIds = new LinkedList<Integer>();
		for(String receiverComponentId : this.receivers.get(streamId)) {
			
			// if(this.groupFields.get(receiverComponentId) != null) {
			computedTaskIds.add(StormConnector.getFieldsGroupingReceiverTaskId(this.topologyContext, this.componentId,
				streamId, receiverComponentId, tuple));
			// }
		}
		LOGGER.trace("tuple: {} -> sentTo ({}): {}", tuple, streamId, computedTaskIds);
		
		for(Integer taskId : computedTaskIds) {
			final Batch buffer = this.outputBuffers.get(streamId).get(taskId);
			buffer.addTuple(tuple);
			
			if(buffer.isFull()) {
				this.batchEmit(streamId, null, buffer, messageId);
				this.outputBuffers.get(streamId).put(
					taskId,
					new Batch(this.batchSize, this.topologyContext.getComponentOutputFields(this.componentId, streamId)
						.size()));
			}
		}
		
		return computedTaskIds;
	}
	
	/**
	 * TODO
	 * 
	 * @param taskId
	 * @param streamId
	 * @param anchors
	 * @param tuple
	 * @param messageId
	 */
	public void tupleEmitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple, Object messageId) {
		final Batch buffer = this.outputBuffers.get(streamId).get(new Integer(taskId));
		buffer.addTuple(tuple);
		
		this.batchEmitDirect(taskId, streamId, anchors, buffer, messageId);
		LOGGER.trace("tuple: {} -> sentTo ({}): {}", tuple, streamId, taskId);
	}
	
	/**
	 * TODO
	 * 
	 * @param streamId
	 * @param anchors
	 * @param batch
	 * @param messageId
	 * 
	 * @return
	 */
	protected abstract List<Integer> batchEmit(String streamId, Collection<Tuple> anchors, Batch batch, Object messageId);
	
	/**
	 * TODO
	 * 
	 * @param taskId
	 * @param streamId
	 * @param anchors
	 * @param batch
	 * @param messageId
	 */
	protected abstract void batchEmitDirect(int taskId, String streamId, Collection<Tuple> anchors, Batch batch, Object messageId);
}
