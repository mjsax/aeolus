/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.hub.cs.dbis.aeolus.batching;

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;





/**
 * TODO
 * 
 * @author Matthias J. Sax
 */
public class InputDebatcher implements IRichBolt {
	private static final long serialVersionUID = 7781347435499103691L;
	
	private final Logger logger = LoggerFactory.getLogger(InputDebatcher.class);
	
	/**
	 * TODO
	 */
	private final IRichBolt wrappedBolt;
	/**
	 * TODO
	 */
	private TopologyContext topologyContext;
	
	
	/**
	 * TODO
	 * 
	 * @param bolt
	 */
	InputDebatcher(IRichBolt bolt) {
		this.wrappedBolt = bolt;
	}
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.topologyContext = context;
		this.wrappedBolt.prepare(stormConf, context, collector);
	}
	
	@Override
	public void execute(Tuple input) {
		this.logger.trace("input: {}", input);
		
		if(input.getValues().getClass().getName().equals(Batch.class.getName())) {
			this.logger.trace("debatching");
			
			final int numberOfAttributes = input.size();
			this.logger.trace("numberOfAttributes: {}", new Integer(numberOfAttributes));
			final BatchColumn[] columns = new BatchColumn[numberOfAttributes];
			
			for(int i = 0; i < numberOfAttributes; ++i) {
				columns[i] = (BatchColumn)input.getValue(i);
			}
			
			final int size = columns[0].size();
			this.logger.trace("batchSize: {}", new Integer(size));
			for(int i = 0; i < size; ++i) {
				final ArrayList<Object> attributes = new ArrayList<Object>(numberOfAttributes);
				
				for(int j = 0; j < numberOfAttributes; ++j) {
					attributes.add(columns[j].get(i));
				}
				this.logger.trace("extracted tuple #{}: {}", new Integer(i), attributes);
				
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
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
