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

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.batching.BatchingOutputFieldsDeclarer;





/**
 * {@link BatchedDeclarer} wraps an {@link BoltDeclarer} and forwards each method call to the wrapped object.
 * Additionally, it calls {@link #directGrouping(String, String)} for calls to "{@code xxxGrouping(...)}" methods
 * (except for direct- and custom-grouping) in order to connect to the direct stream that is declares by
 * {@link SpoutOutputBatcher} or {@link BoltOutputBatcher}.
 * 
 * @author mjsax
 */
class BatchedDeclarer implements BoltDeclarer {
	/**
	 * The original declarer provided by {@link TopologyBuilder}.
	 */
	private final BoltDeclarer declarer;
	
	
	
	/**
	 * Instantiate a new {@link BatchedDeclarer} that wraps the provided {@link BoltDeclarer}.
	 * 
	 * @param declarer
	 *            The {@link BoltDeclarer} to be wrapped.
	 */
	public BatchedDeclarer(BoltDeclarer declarer) {
		this.declarer = declarer;
	}
	
	
	
	@Override
	public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
		return this.fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
	}
	
	@Override
	public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
		this.declarer.fieldsGrouping(componentId, fields).directGrouping(componentId,
			BatchingOutputFieldsDeclarer.STREAM_PREFIX + streamId);
		return this;
	}
	
	@Override
	public BoltDeclarer globalGrouping(String componentId) {
		return this.globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public BoltDeclarer globalGrouping(String componentId, String streamId) {
		this.declarer.globalGrouping(componentId, streamId).directGrouping(componentId,
			BatchingOutputFieldsDeclarer.STREAM_PREFIX + streamId);
		return this;
	}
	
	@Override
	public BoltDeclarer shuffleGrouping(String componentId) {
		return this.shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
		this.declarer.shuffleGrouping(componentId, streamId).directGrouping(componentId,
			BatchingOutputFieldsDeclarer.STREAM_PREFIX + streamId);
		return this;
	}
	
	@Override
	public BoltDeclarer localOrShuffleGrouping(String componentId) {
		return this.localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
		this.declarer.localOrShuffleGrouping(componentId, streamId).directGrouping(componentId,
			BatchingOutputFieldsDeclarer.STREAM_PREFIX + streamId);
		return this;
	}
	
	@Override
	public BoltDeclarer noneGrouping(String componentId) {
		return this.noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public BoltDeclarer noneGrouping(String componentId, String streamId) {
		this.declarer.noneGrouping(componentId, streamId).directGrouping(componentId,
			BatchingOutputFieldsDeclarer.STREAM_PREFIX + streamId);
		return this;
	}
	
	@Override
	public BoltDeclarer allGrouping(String componentId) {
		return this.allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public BoltDeclarer allGrouping(String componentId, String streamId) {
		this.declarer.allGrouping(componentId, streamId).directGrouping(componentId,
			BatchingOutputFieldsDeclarer.STREAM_PREFIX + streamId);
		return this;
	}
	
	@Override
	public BoltDeclarer directGrouping(String componentId) {
		return this.directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public BoltDeclarer directGrouping(String componentId, String streamId) {
		this.declarer.directGrouping(componentId, streamId);
		return this;
	}
	
	@Override
	public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
		return this.customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
	}
	
	@Override
	public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
		this.declarer.customGrouping(componentId, streamId, grouping);
		return this;
	}
	
	@Override
	public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
		this.declarer.grouping(id, grouping);
		return this;
	}
	
	@Override
	public BoltDeclarer addConfigurations(@SuppressWarnings("rawtypes") Map conf) {
		this.declarer.addConfigurations(conf);
		return this;
	}
	
	@Override
	public BoltDeclarer addConfiguration(String config, Object value) {
		this.addConfiguration(config, value);
		return this;
	}
	
	@Override
	public BoltDeclarer setDebug(boolean debug) {
		this.declarer.setDebug(debug);
		return this;
	}
	
	@Override
	public BoltDeclarer setMaxTaskParallelism(Number val) {
		this.declarer.setMaxTaskParallelism(val);
		return this;
	}
	
	@Override
	public BoltDeclarer setMaxSpoutPending(Number val) {
		this.declarer.setMaxSpoutPending(val);
		return this;
	}
	
	@Override
	public BoltDeclarer setNumTasks(Number val) {
		this.declarer.setNumTasks(val);
		return this;
	}
	
}
