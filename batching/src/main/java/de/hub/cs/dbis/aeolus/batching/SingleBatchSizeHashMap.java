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
import java.util.Map;
import java.util.Set;





/**
 * {@link SingleBatchSizeHashMap} is a dummy map, that stores a single batch size and returns this value for any key
 * (ie, stream ID) used in {@link #get(Object)}. All other methods may throw an {@link UnsupportedOperationException}.
 * 
 * 
 * @author Matthias J. Sax
 */
class SingleBatchSizeHashMap extends HashMap<String, Integer> {
	private static final long serialVersionUID = -1872488162485981597L;
	
	/**
	 * The batch size.
	 */
	private final Integer batchSize;
	
	
	
	/**
	 * Instantiates a new {@link SingleBatchSizeHashMap} that return {@code batchSize} for any stream ID.
	 * 
	 * @param batchSize
	 *            The batch size to be returned.
	 */
	public SingleBatchSizeHashMap(int batchSize) {
		this.batchSize = new Integer(batchSize);
	}
	
	
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public int size() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public boolean containsKey(Object key) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Return {@link #batchSize} for any given key(
	 */
	@Override
	public Integer get(Object key) {
		return this.batchSize;
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException} if {@code value} is different to the already specified batch
	 * size.
	 */
	@Override
	public Integer put(String key, Integer value) {
		if(value == null || value.intValue() != this.batchSize.intValue()) {
			throw new UnsupportedOperationException();
		}
		return this.batchSize;
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public Integer remove(Object key) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public void putAll(Map<? extends String, ? extends Integer> m) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public Set<String> keySet() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public Collection<Integer> values() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Throws an {@link UnsupportedOperationException}.
	 */
	@Override
	public Set<java.util.Map.Entry<String, Integer>> entrySet() {
		throw new UnsupportedOperationException();
	}
	
}
