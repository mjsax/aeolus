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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
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
	protected final static Logger logger = LoggerFactory.getLogger(AbstractBatchCollector.class);
	
	/**
	 * The size of the output batches.
	 */
	private final int batchSize;
	/**
	 * The number of the attributes of the output schema.
	 */
	private final Map<String, Integer> numberOfAttributes = new HashMap<String, Integer>();
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
	private final Map<String, List<String>> receivers = new HashMap<String, List<String>>();
	/**
	 * Contains all receivers, that use fields-grouping.
	 */
	private final Set<String> fieldsGroupingReceivers = new HashSet<String>();
	/**
	 * Maps each output stream to a task-id-to-batch-index map.
	 * 
	 * Task-IDs can have arbitrary values but we need values from 0 to number-of-batches. Thus, we assign an appropriate
	 * index value to each task-ID.
	 */
	private final Map<String, Map<Integer, Integer>> streamBatchIndexMapping = new HashMap<String, Map<Integer, Integer>>();
	/**
	 * Maps output streams to corresponding output buffers.
	 * 
	 * The number of outputBuffers depends on the number of logical receivers as well as each logical receiver's
	 * distribution pattern and parallelism.
	 */
	private final Map<String, Batch[]> outputBuffers = new HashMap<String, Batch[]>();
	/**
	 * Assigns a "weight" to each receiver the used fields-grouping. This weight is necessary to compute the correct
	 * index within the list of output buffers.
	 */
	private final Map<String, Integer> weights = new HashMap<String, Integer>();
	
	
	
	/**
	 * Creates a new {@link AbstractBatchCollector} that emits batches of size {@code batchSize}.
	 * 
	 * @param context
	 *            The current runtime environment.
	 * @param batchSize
	 *            The size of the output batches to be built.
	 */
	AbstractBatchCollector(TopologyContext context, int batchSize) {
		logger.trace("batchSize: {}", new Integer(batchSize));
		
		this.batchSize = batchSize;
		this.topologyContext = context;
		this.componentId = context.getThisComponentId();
		logger.trace("this-id: {}", this.componentId);
		
		// StreamId -> ComponentId -> Grouping
		for(Entry<String, Map<String, Grouping>> outputStream : context.getThisTargets().entrySet()) {
			final String streamId = outputStream.getKey();
			logger.trace("output-stream: {}", streamId);
			final Map<String, Grouping> streamReceivers = outputStream.getValue();
			
			final int numAttributes = context.getComponentOutputFields(this.componentId, streamId).size();
			this.numberOfAttributes.put(streamId, new Integer(numAttributes));
			
			int numberOfBatches = 1;
			final ArrayList<String> receiverIds = new ArrayList<String>(streamReceivers.size());
			this.receivers.put(streamId, receiverIds);
			
			for(Entry<String, Grouping> receiver : streamReceivers.entrySet()) {
				final String receiverId = receiver.getKey();
				receiverIds.add(receiverId);
				final List<Integer> taskIds = context.getComponentTasks(receiverId);
				logger.trace("receiver and tasks: {} - {}", receiverId, taskIds);
				if(receiver.getValue().is_set_fields()) {
					// TODO we could reduce number of output buffers, if two logical consumers use the same output field
					// for partitioning AND have the same dop
					this.fieldsGroupingReceivers.add(receiverId);
					logger.trace("fieldsGrouping");
					
					this.weights.put(receiverId, new Integer(numberOfBatches));
					numberOfBatches *= taskIds.size();
					
					Map<Integer, Integer> taskToIndex = this.streamBatchIndexMapping.get(streamId);
					if(taskToIndex == null) {
						taskToIndex = new HashMap<Integer, Integer>();
						this.streamBatchIndexMapping.put(streamId, taskToIndex);
					}
					int i = 0;
					for(Integer tId : taskIds) {
						taskToIndex.put(tId, new Integer(i));
						++i;
					}
				}
			}
			
			
			Batch[] batches = new Batch[numberOfBatches];
			for(int i = 0; i < numberOfBatches; ++i) {
				batches[i] = new Batch(batchSize, numAttributes);
			}
			this.outputBuffers.put(streamId, batches);
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
		int bufferIndex = 0;
		
		if(this.streamBatchIndexMapping.get(streamId) != null) {
			Map<Integer, Integer> taskIndex = this.streamBatchIndexMapping.get(streamId);
			for(String receiverComponentId : this.receivers.get(streamId)) {
				if(this.fieldsGroupingReceivers.contains(receiverComponentId)) {
					Integer taskId = StormConnector.getFieldsGroupingReceiverTaskId(this.topologyContext,
						this.componentId, streamId, receiverComponentId, tuple);
					bufferIndex += (this.weights.get(receiverComponentId).intValue() * taskIndex.get(taskId).intValue());
				}
			}
		}
		
		final Batch buffer = this.outputBuffers.get(streamId)[bufferIndex];
		buffer.addTuple(tuple);
		
		if(buffer.isFull()) {
			this.batchEmit(streamId, null, buffer, null);
			this.outputBuffers.get(streamId)[bufferIndex] = new Batch(this.batchSize, this.numberOfAttributes.get(
				streamId).intValue());
		}
		
		return null;
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
		// final Batch buffer = this.outputBuffers.get(streamId).get(new Integer(taskId));
		// buffer.addTuple(tuple);
		//
		// this.batchEmitDirect(taskId, streamId, anchors, buffer, messageId);
		// logger.trace("tuple: {} -> sentTo ({}): {}", tuple, streamId, new Integer(taskId));
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
