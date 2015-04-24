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

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;





/**
 * @author Matthias J. Sax
 */
public class SingleBatchSizeHashMapTest {
	
	@Test
	public void testGet() {
		final long seed = System.currentTimeMillis();
		System.out.println("seed: " + seed);
		Random r = new Random(seed);
		
		final int batchSize = r.nextInt();
		Assert.assertEquals(new Integer(batchSize), new SingleBatchSizeHashMap(batchSize).get(null));
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testSize() {
		new SingleBatchSizeHashMap(0).size();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testIsEmpty() {
		new SingleBatchSizeHashMap(0).isEmpty();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testContainsKey() {
		new SingleBatchSizeHashMap(0).containsKey(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testContainsValue() {
		new SingleBatchSizeHashMap(0).containsValue(null);
	}
	
	@Test
	public void testPut() {
		new SingleBatchSizeHashMap(0).put(null, new Integer(0));
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testPutFail1() {
		new SingleBatchSizeHashMap(0).put(null, null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testPutFail2() {
		new SingleBatchSizeHashMap(0).put(null, new Integer(1));
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testRemove() {
		new SingleBatchSizeHashMap(0).remove(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testPutAll() {
		new SingleBatchSizeHashMap(0).putAll(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testClear() {
		new SingleBatchSizeHashMap(0).clear();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testKeySet() {
		new SingleBatchSizeHashMap(0).keySet();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testValues() {
		new SingleBatchSizeHashMap(0).values();
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testEntrySet() {
		new SingleBatchSizeHashMap(0).entrySet();
	}
}
