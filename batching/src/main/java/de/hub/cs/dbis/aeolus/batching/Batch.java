package de.hub.cs.dbis.aeolus.batching;

/*
 * #%L
 * batching
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * TODO write this at the very last -> before check consistent comments in all other classes -> check consistent asserts
 * in all other classes-> check consistent logging (debug/trace) in all other classes -> check final modifiers
 * 
 * @author Matthias J. Sax
 */
class Batch extends ArrayList<BatchColumn> {
	private static final long serialVersionUID = 4904732830895959975L;
	
	private final Logger logger = LoggerFactory.getLogger(Batch.class);
	
	/**
	 * The capacity of this {@link Batch}
	 */
	private int batchSize;
	/**
	 * The number of attributes of tuple's buffered in this {@link Batch}.
	 */
	private int numberOfAttributes;
	/**
	 * The current number of tuples stored in this {@link Batch}.
	 */
	private int size;
	
	
	
	/**
	 * Default constructor. Needed for deserialization.
	 */
	public Batch() {
		super();
	}
	
	/**
	 * TODO
	 * 
	 * @param batchSize
	 * @param numberOfAttributes
	 */
	Batch(int batchSize, int numberOfAttributes) {
		super(numberOfAttributes);
		
		assert (batchSize > 0);
		assert (numberOfAttributes > 0);
		
		this.logger.debug("batchSize: {}; numberOfAttributes: {}", new Integer(batchSize), new Integer(
			numberOfAttributes));
		
		this.batchSize = batchSize;
		this.numberOfAttributes = numberOfAttributes;
		this.size = 0;
		
		for(int i = 0; i < numberOfAttributes; ++i) {
			this.add(new BatchColumn(batchSize));
		}
	}
	
	/**
	 * TODO
	 * 
	 * @param tuple
	 */
	void addTuple(List<Object> tuple) { // cannot use backtype.storm.tuple.Tuple or backtype.storm.tuple.Values because
										// Collector.emit(...) uses "List<Object> tuple" as parameter
		assert (tuple != null);
		assert (tuple.size() == this.numberOfAttributes);
		assert (this.size < this.batchSize);
		
		this.logger.trace("tuple: {}; size before insert: {}", tuple, new Integer(this.size));
		
		for(int i = 0; i < this.numberOfAttributes; ++i) {
			this.get(i).add(tuple.get(i));
		}
		
		++this.size;
	}
	
	/**
	 * Returns {@code true} if this batch is full; {@code false} otherwise.
	 * 
	 * @return {@code true}, if this batch is full; {@code false} otherwise.
	 */
	public boolean isFull() {
		return this.size == this.batchSize;
	}
	
}
