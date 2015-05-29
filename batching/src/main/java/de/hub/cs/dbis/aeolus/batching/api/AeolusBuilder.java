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
package de.hub.cs.dbis.aeolus.batching.api;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.IRichStateSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;





/**
 * TODO
 * 
 * @author Matthias J. Sax
 */
public class AeolusBuilder extends TopologyBuilder {
	
	@Override
	public BoltDeclarer setBolt(String id, IRichBolt bolt) {
		return this.setBolt(id, bolt, null, 0);
	}
	
	/**
	 * Define a new bolt in this topology with parallelism of just one thread.
	 * 
	 * @param id
	 *            The id of this component. This id is referenced by other components that want to consume this bolt's
	 *            outputs.
	 * @param bolt
	 *            The bolt to be added to the topology.
	 * @param batchSize
	 *            The batch size to be used for all output streams of the given spout (must not be negative).
	 * 
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(String id, IRichBolt bolt, int batchSize) {
		return this.setBolt(id, bolt, null, batchSize);
	}
	
	/**
	 * Define a new bolt in this topology with parallelism of just one thread.
	 * 
	 * @param id
	 *            The id of this component. This id is referenced by other components that want to consume this bolt's
	 *            outputs.
	 * @param bolt
	 *            The bolt to be added to the topology.
	 * @param batchSizes
	 *            Specifies different batch sizes for different output streams. If the given bolt declares an output
	 *            stream, that is not specified in this map, the output tuples will not be batched. The specified batch
	 *            sizes must not be negative.
	 * 
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(String id, IRichBolt bolt, HashMap<String, Integer> batchSizes) {
		return this.setBolt(id, bolt, null, batchSizes);
	}
	
	@Override
	public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
		return this.setBolt(id, bolt, parallelism_hint, 0);
	}
	
	/**
	 * Define a new bolt in this topology with the specified amount of parallelism.
	 * 
	 * @param id
	 *            The id of this component. This id is referenced by other components that want to consume this bolt's
	 *            outputs.
	 * @param bolt
	 *            The bolt to be added to the topology.
	 * @param parallelism_hint
	 *            The number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a
	 *            process somewhere around the cluster.
	 * @param batchSize
	 *            The batch size to be used for all output streams of the given spout (must not be negative).
	 * 
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint, int batchSize) {
		bolt = new InputDebatcher(bolt);
		if(batchSize > 0) {
			bolt = new BoltOutputBatcher(bolt, batchSize);
		} else {
			bolt = new BoltOutputBatcher(bolt, new HashMap<String, Integer>());
		}
		return new BatchedDeclarer(super.setBolt(id, bolt, parallelism_hint));
	}
	
	/**
	 * Define a new bolt in this topology with the specified amount of parallelism.
	 * 
	 * @param id
	 *            The id of this component. This id is referenced by other components that want to consume this bolt's
	 *            outputs.
	 * @param bolt
	 *            The bolt to be added to the topology.
	 * @param parallelism_hint
	 *            The number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a
	 *            process somewhere around the cluster.
	 * @param batchSizes
	 *            Specifies different batch sizes for different output streams. If the given bolt declares an output
	 *            stream, that is not specified in this map, the output tuples will not be batched. The specified batch
	 *            sizes must not be negative.
	 * 
	 * @return use the returned object to declare the inputs to this component
	 */
	public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint, HashMap<String, Integer> batchSizes) {
		if(batchSizes == null) {
			batchSizes = new HashMap<String, Integer>();
		}
		return new BatchedDeclarer(super.setBolt(id, new BoltOutputBatcher(new InputDebatcher(bolt), batchSizes),
			parallelism_hint));
	}
	
	/**
	 * Not supported yet. Throws an {@link UnsupportedOperationException}.
	 * 
	 * @throws UnsupportedOperationException
	 */
	@Override
	public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Not supported yet. Throws an {@link UnsupportedOperationException}.
	 * 
	 * @throws UnsupportedOperationException
	 */
	@Override
	public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public SpoutDeclarer setSpout(String id, IRichSpout spout) {
		return this.setSpout(id, spout, null, null);
	}
	
	/**
	 * Define a new spout in this topology.
	 * 
	 * @param id
	 *            The ID of this component. This ID is referenced by other components that want to consume this spout's
	 *            outputs.
	 * @param spout
	 *            The spout to be added to the topology.
	 * @param batchSize
	 *            The batch size to be used for all output streams of the given spout (must not be negative).
	 */
	public SpoutDeclarer setSpout(String id, IRichSpout spout, int batchSize) {
		return this.setSpout(id, spout, null, batchSize);
	}
	
	/**
	 * Define a new spout in this topology.
	 * 
	 * @param id
	 *            The ID of this component. This ID is referenced by other components that want to consume this spout's
	 *            outputs.
	 * @param spout
	 *            The spout to be added to the topology.
	 * @param batchSizes
	 *            Specifies different batch sizes for different output streams. If the given spout declares an output
	 *            stream, that is not specified in this map, the output tuples will not be batched. The specified batch
	 *            sizes must not be negative.
	 */
	public SpoutDeclarer setSpout(String id, IRichSpout spout, Map<String, Integer> batchSizes) {
		return this.setSpout(id, spout, null, batchSizes);
	}
	
	/**
	 * Define a new spout in this topology.
	 * 
	 * @param id
	 *            The ID of this component. This ID is referenced by other components that want to consume this spout's
	 *            outputs.
	 * @param spout
	 *            The spout to be added to the topology.
	 * @param parallelism_hint
	 *            The number of tasks that should be assigned to execute this spout. Each task will run on a thread in a
	 *            process somewhere around the cluster.
	 * @param batchSize
	 *            The batch size to be used for all output streams of the given spout (must not be negative).
	 */
	public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint, int batchSize) {
		if(batchSize <= 0) {
			return super.setSpout(id, new SpoutOutputBatcher(spout, new HashMap<String, Integer>()), parallelism_hint);
		}
		
		return super.setSpout(id, new SpoutOutputBatcher(spout, batchSize), parallelism_hint);
	}
	
	/**
	 * Define a new spout in this topology.
	 * 
	 * @param id
	 *            The ID of this component. This ID is referenced by other components that want to consume this spout's
	 *            outputs.
	 * @param spout
	 *            The spout to be added to the topology.
	 * @param parallelism_hint
	 *            The number of tasks that should be assigned to execute this spout. Each task will run on a thread in a
	 *            process somewhere around the cluster.
	 * @param batchSizes
	 *            Specifies different batch sizes for different output streams. If the given spout declares an output
	 *            stream, that is not specified in this map, the output tuples will not be batched. The specified batch
	 *            sizes must not be negative.
	 */
	public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint, Map<String, Integer> batchSizes) {
		if(batchSizes == null) {
			batchSizes = new HashMap<String, Integer>();
		}
		return super.setSpout(id, new SpoutOutputBatcher(spout, batchSizes), parallelism_hint);
	}
	
	@Override
	public void setStateSpout(String id, IRichStateSpout stateSpout) {
		throw new UnsupportedOperationException("Not supported by Storm (0.9.3) yet.");
	}
	
	@Override
	public void setStateSpout(String id, IRichStateSpout stateSpout, Number parallelism_hint) {
		throw new UnsupportedOperationException("Not supported by Storm (0.9.3) yet.");
	}
	
}
