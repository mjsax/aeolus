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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * {@link BatchColumn} represent an attribute column of a {@link Batch}.
 * 
 * @author Matthias J. Sax
 */
class BatchColumn extends ArrayList<Object> {
	private static final long serialVersionUID = -2215147192473477343L;
	
	private final static Logger logger = LoggerFactory.getLogger(BatchColumn.class);
	
	
	
	/**
	 * Default constructor. Needed for serialization.
	 */
	public BatchColumn() {
		super();
	}
	
	/**
	 * Instantiates a {@link BatchColumn} with size {@code batchSize} (must be larger than zero).
	 * 
	 * @param batchSize
	 *            The number of attributes that should be stored in this {@link BatchColumn}.
	 */
	public BatchColumn(int batchSize) {
		super(batchSize);
		assert (batchSize > 0);
		
		if(batchSize == 1 && logger.isWarnEnabled()) {
			logger.warn("Instantiating a BatchColumn of size 1.");
		}
	}
	
	/**
	 * Return the hash value of the {@link BatchColumn}. To compute the hash value, only a single attribute value is
	 * considered. This is important in order to 'hide' a {@code Batch} from Storm and ensures, that all batches are
	 * sent to the correct consumer tasks.
	 * 
	 * The considered attribute could be any of the stored one. This implementation uses the first attribute that is
	 * inserted.
	 */
	@Override
	public int hashCode() {
		assert (this.size() > 0);
		return this.get(0).hashCode();
	}
	
}
