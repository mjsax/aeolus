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
package de.hub.cs.dbis.lrb.toll;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;





/**
 * a {@link TollDataStore} which stores data in memory. Due to the fact that data to satisfy historical LRB query can
 * get huge, you're advised to use it carefully, only.
 * 
 * This implementation is not thread-safe.
 * 
 * @author richter
 */
/*
 * This class is almost used exclusively for tests, but it doesn't hurt to share it to be users started easier.
 */
public class MemoryTollDataStore implements TollDataStore {
	private final Map<Triple<Integer, Integer, Integer>, Integer> store = new HashMap<Triple<Integer, Integer, Integer>, Integer>();
	private final MutableTriple<Integer, Integer, Integer> reusableMapKey = new MutableTriple<Integer, Integer, Integer>(
		0, 0, 0);
	
	// avoid the allocation of memory for every key
	
	/**
	 * {@inheritDoc }
	 */
	@Override
	public Integer retrieveToll(int xWay, int day, int vehicleIdentifier) {
		reusableMapKey.setLeft(xWay);
		reusableMapKey.setMiddle(day);
		reusableMapKey.setRight(vehicleIdentifier);
		Integer toll = store.get(reusableMapKey);
		return toll;
	}
	
	/**
	 * {@inheritDoc }
	 */
	@Override
	public void storeToll(int xWay, int day, int vehicleIdentifier, int toll) {
		reusableMapKey.setLeft(xWay);
		reusableMapKey.setMiddle(day);
		reusableMapKey.setRight(vehicleIdentifier);
		store.put(reusableMapKey, toll);
	}
	
	@Override
	public Integer removeEntry(int xWay, int day, int vehicleIdentifier) {
		reusableMapKey.setLeft(xWay);
		reusableMapKey.setMiddle(day);
		reusableMapKey.setRight(vehicleIdentifier);
		Integer toll = store.remove(reusableMapKey);
		return toll;
	}
	
}
