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

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * {@link BoltOutputBatcher} wraps a bolt, buffers the bolt output tuples, and emits those tuples in batches. All
 * receiving bolts must be wrapped by an {@link InputDebatcher}.<br/>
 * <br/>
 * <strong>CAUTION:</strong>Calls to {@code .emit(...)} will return {@code null}, because the tuples might still be in
 * the output buffer and not transfered yet.<br />
 * <br />
 * <strong>CAUTION:</strong>Tuple acking, failing, and anchoring is currently not supported.
 * 
 * @author Matthias J. Sax
 */
public class BoltOutputBatcher implements IRichBolt {
	private final static long serialVersionUID = 6453060658895879104L;
	
	/**
	 * The used {@link BatchOutputCollector} that wraps the actual {@link OutputCollector}.
	 */
	private BatchOutputCollector batchCollector;
	/**
	 * The bolt that is wrapped.
	 */
	private final IRichBolt wrappedBolt;
	/**
	 * The sizes of the output batches for each output stream.
	 */
	private final Map<String, Integer> batchSizes;
	/**
	 * The size of the output batches (for all output streams).
	 */
	private final int batchSize;
	
	
	
	/**
	 * Instantiates a new {@link BoltOutputBatcher} the emits the output of the given {@link IRichBolt} in batches of
	 * size {@code batchSize}.
	 * 
	 * @param bolt
	 *            The original bolt to be wrapped.
	 * @param batchSize
	 *            The batch size to be used for all output streams.
	 */
	public BoltOutputBatcher(IRichBolt bolt, int batchSize) {
		this.wrappedBolt = bolt;
		this.batchSizes = null;
		this.batchSize = batchSize;
	}
	
	/**
	 * Instantiates a new {@link BoltOutputBatcher} the emits the output of the given {@link IRichBolt} in batches of
	 * size {@code batchSize}.
	 * 
	 * @param bolt
	 *            The original bolt to be wrapped.
	 * @param batchSizes
	 *            The batch sizes for each output stream.
	 */
	public BoltOutputBatcher(IRichBolt bolt, Map<String, Integer> batchSizes) {
		this.wrappedBolt = bolt;
		this.batchSizes = batchSizes;
		this.batchSize = -1;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		if(this.batchSizes != null) {
			this.batchCollector = new BatchOutputCollector(context, collector, this.batchSizes);
		} else {
			this.batchCollector = new BatchOutputCollector(context, collector, this.batchSize);
		}
		this.wrappedBolt.prepare(stormConf, context, this.batchCollector);
		
	}
	
	@Override
	public void execute(Tuple input) {
		this.wrappedBolt.execute(input);
	}
	
	@Override
	public void cleanup() {
		this.wrappedBolt.cleanup();
		this.batchCollector.flush();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.wrappedBolt.declareOutputFields(new BatchingOutputFieldsDeclarer(declarer));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedBolt.getComponentConfiguration();
	}
	
	/**
	 * Registers internally used classes for serialization and deserialization. Same as
	 * {@link SpoutOutputBatcher#registerKryoClasses(Config)} and {@link InputDebatcher#registerKryoClasses(Config)}.
	 * 
	 * @param stormConfig
	 *            The storm config the which the classes should be registered to.
	 */
	public static void registerKryoClasses(Config stormConfig) {
		AbstractBatchCollector.registerKryoClasses(stormConfig);
	}
	
}
