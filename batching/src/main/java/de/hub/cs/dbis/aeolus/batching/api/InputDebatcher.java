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

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import de.hub.cs.dbis.aeolus.batching.AbstractBatchCollector;
import de.hub.cs.dbis.aeolus.batching.BatchColumn;





/**
 * {@link InputDebatcher} enables a bolt to receives input from a producer spout or bolt that emits batches via
 * {@link SpoutOutputBatcher} or {@link BoltOutputBatcher}, respectively. {@link InputDebatcher} extracts each tuple of
 * a batch and forwards it to its wrapped bolt for processing. {@link InputDebatcher} can handle and combination of
 * batched and non-batched input and works with any batch size.<br />
 * <br />
 * <strong>CAUTION:</strong>Tuple acking, failing, and anchoring is currently not supported.
 * 
 * @author mjsax
 */
public class InputDebatcher implements IRichBolt {
	private final static long serialVersionUID = 7781347435499103691L;
	
	private final static Logger logger = LoggerFactory.getLogger(InputDebatcher.class);
	
	/**
	 * The bolt that is wrapped.
	 */
	private final IRichBolt wrappedBolt;
	/**
	 * The current runtime environment.
	 */
	private TopologyContext topologyContext;
	
	
	
	/**
	 * Instantiates a new {@link InputDebatcher} that wraps the given bolt.
	 * 
	 * @param bolt
	 *            The bolt to be wrapped.
	 */
	public InputDebatcher(IRichBolt bolt) {
		this.wrappedBolt = bolt;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.topologyContext = context;
		this.wrappedBolt.prepare(stormConf, context, collector);
	}
	
	/**
	 * Processes a single input tuple or batch. In case of an regular input tuple, the input tuple is simply forwarded
	 * to the wrapped bolt for processing. In case of an input batch, all tuples are extracted from the batch and
	 * forwarded to the wrapped bolt one by one. The batch metadata is recreated for each extracted tuple.
	 */
	@Override
	public void execute(Tuple input) {
		logger.trace("input: {}", input);
		
		// we cannot check "input.getValues() instanceof Batch", because Storm does not preserve this information
		// (the class of input.getValues() is always java.utils.ArrayList)
		if(input.size() > 0 && input.getValue(0) instanceof BatchColumn) {
			logger.trace("debatching");
			
			final int numberOfAttributes = input.size();
			logger.trace("numberOfAttributes: {}", new Integer(numberOfAttributes));
			final BatchColumn[] columns = new BatchColumn[numberOfAttributes];
			
			for(int i = 0; i < numberOfAttributes; ++i) {
				columns[i] = (BatchColumn)input.getValue(i);
			}
			
			final int size = columns[0].size();
			logger.trace("batchSize: {}", new Integer(size));
			for(int i = 0; i < size; ++i) {
				final ArrayList<Object> attributes = new ArrayList<Object>(numberOfAttributes);
				
				for(int j = 0; j < numberOfAttributes; ++j) {
					attributes.add(columns[j].get(i));
				}
				logger.trace("extracted tuple #{}: {}", new Integer(i), attributes);
				
				final TupleImpl tuple = new TupleImpl(this.topologyContext, attributes, input.getSourceTask(),
					input.getSourceStreamId());
				
				this.wrappedBolt.execute(tuple);
			}
		} else {
			this.wrappedBolt.execute(input);
		}
		
	}
	
	@Override
	public void cleanup() {
		this.wrappedBolt.cleanup();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.wrappedBolt.declareOutputFields(declarer);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.wrappedBolt.getComponentConfiguration();
	}
	
	/**
	 * Registers internally used classes for serialization and deserialization. Same as
	 * {@link SpoutOutputBatcher#registerKryoClasses(Config)} and {@link BoltOutputBatcher#registerKryoClasses(Config)}.
	 * 
	 * @param stormConfig
	 *            The storm config the which the classes should be registered to.
	 */
	public static void registerKryoClasses(Config stormConfig) {
		AbstractBatchCollector.registerKryoClasses(stormConfig);
	}
	
}
