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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;





/**
 * @author Matthias J. Sax
 */
public class TestBatchCollector extends AbstractBatchCollector {
	public final Map<String, List<Integer>> taskBuffer = new HashMap<String, List<Integer>>();
	public final Map<String, List<Collection<Tuple>>> anchorBuffer = new HashMap<String, List<Collection<Tuple>>>();
	public final Map<String, List<Batch>> batchBuffer = new HashMap<String, List<Batch>>();
	public final Map<String, List<Object>> messageIdBuffer = new HashMap<String, List<Object>>();
	
	
	
	TestBatchCollector(TopologyContext context, int batchSize) {
		super(context, batchSize);
	}
	
	TestBatchCollector(TopologyContext context, HashMap<String, Integer> batchSizes) {
		super(context, batchSizes);
	}
	
	
	
	@Override
	protected List<Integer> batchEmit(String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
		this.setListMembers(-1, streamId, anchors, batch, messageId);
		return null;
	}
	
	@Override
	protected void batchEmitDirect(int taskId, String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
		this.setListMembers(taskId, streamId, anchors, batch, messageId);
		
	}
	
	private void setListMembers(int taskId, String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
		if(taskId != -1) {
			List<Integer> taksList = this.taskBuffer.get(streamId);
			if(taksList == null) {
				taksList = new LinkedList<Integer>();
				this.taskBuffer.put(streamId, taksList);
			}
			taksList.add(new Integer(taskId));
		}
		
		List<Collection<Tuple>> anchorList = this.anchorBuffer.get(streamId);
		if(anchorList == null) {
			anchorList = new LinkedList<Collection<Tuple>>();
			this.anchorBuffer.put(streamId, anchorList);
		}
		anchorList.add(anchors);
		
		List<Batch> batchList = this.batchBuffer.get(streamId);
		if(batchList == null) {
			batchList = new LinkedList<Batch>();
			this.batchBuffer.put(streamId, batchList);
		}
		batchList.add(batch);
		
		List<Object> messageIdList = this.messageIdBuffer.get(streamId);
		if(messageIdList == null) {
			messageIdList = new LinkedList<Object>();
			this.messageIdBuffer.put(streamId, messageIdList);
		}
		messageIdList.add(messageId);
	}
}
