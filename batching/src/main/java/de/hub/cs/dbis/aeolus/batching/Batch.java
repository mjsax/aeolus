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

import backtype.storm.tuple.Values;





/**
 * A {@link Batch} is a buffer that stores multiple tuples (ie, {@link Values}). It requires that all tuples have the
 * same number of attributes.
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
	 * The number of attributes buffered in this {@link Batch}.
	 */
	private int numberOfAttributes;
	/**
	 * The current number of tuples stored in this {@link Batch}.
	 */
	private int size;
	
	
	
	/**
	 * Default constructor. Needed for serialization.
	 */
	public Batch() {
		super();
	}
	
	/**
	 * Instantiates a new {@link Batch} with size {@code batchSize}. All tuples that are inserted, must have the same
	 * number of attributes as specified by {@code numberOfAttributes}.
	 * 
	 * @param batchSize
	 *            The number of tuples that can be stored in this {@link Batch} (must be largen than 0).
	 * @param numberOfAttributes
	 *            The number of attributes of the tuples stored in this {@link Batch} (must be larger than 0).
	 */
	Batch(int batchSize, int numberOfAttributes) {
		super(numberOfAttributes);
		
		assert (batchSize > 0);
		assert (numberOfAttributes > 0);
		
		this.logger.debug("batchSize: {}; numberOfAttributes: {}", new Integer(batchSize), new Integer(
			numberOfAttributes));
		
		if(batchSize == 1 && this.logger.isWarnEnabled()) {
			this.logger.warn("Instantiating a Batch of size 1.");
		}
		this.batchSize = batchSize;
		this.numberOfAttributes = numberOfAttributes;
		this.size = 0;
		
		for(int i = 0; i < numberOfAttributes; ++i) {
			this.add(new BatchColumn(batchSize));
		}
	}
	
	
	
	/**
	 * Appends a new tuple to the {@link Batch}.
	 * 
	 * @param tuple
	 *            The tuple to be added.
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
