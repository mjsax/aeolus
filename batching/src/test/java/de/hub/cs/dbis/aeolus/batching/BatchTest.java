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

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.tuple.Values;





/**
 * @author Matthias J. Sax
 */
public class BatchTest {
	private long seed;
	private Random r;
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	@Test
	public void testBatch() {
		final int batchSize = 1 + this.r.nextInt(20);
		final int numberOfAttributes = 1 + this.r.nextInt(5);
		
		final int[] type = new int[numberOfAttributes];
		for(int j = 0; j < numberOfAttributes; ++j) {
			// 0 == int
			// 1 == double
			// 2 == char
			type[j] = this.r.nextInt(3);
		}
		
		Batch b = new Batch(batchSize, numberOfAttributes);
		
		for(int i = 0; i < batchSize; ++i) {
			Values tuple = new Values();
			for(int j = 0; j < numberOfAttributes; ++j) {
				switch(type[j]) {
				case 0:
					tuple.add(j, new Integer(this.r.nextInt()));
					break;
				case 1:
					tuple.add(j, new Double(this.r.nextDouble() + this.r.nextLong()));
					break;
				default:
					tuple.add(j, new String("" + (char)(32 + this.r.nextInt(95))));
				}
			}
			b.addTuple(tuple);
			if(i < batchSize - 1) {
				Assert.assertFalse(b.isFull());
			} else {
				Assert.assertTrue(b.isFull());
			}
			
		}
	}
	
	@Test(expected = AssertionError.class)
	public void testInvalidNumberOfAttributesToSmall() {
		final int numberOfAttributes = 1 + this.r.nextInt(5);
		
		Batch b = new Batch(2, numberOfAttributes);
		
		Values tuple = new Values();
		for(int i = 0; i < numberOfAttributes - 1; ++i) {
			tuple.add(new Integer(0));
		}
		
		b.addTuple(new Values());
	}
	
	@Test(expected = AssertionError.class)
	public void testInvalidNumberOfAttributesToLarge() {
		final int numberOfAttributes = 1 + this.r.nextInt(5);
		
		Batch b = new Batch(2, numberOfAttributes);
		
		Values tuple = new Values();
		for(int i = 0; i < numberOfAttributes + 1; ++i) {
			tuple.add(new Integer(0));
		}
		
		b.addTuple(new Values());
	}
	
	@Test(expected = AssertionError.class)
	public void testNullTuple() {
		Batch b = new Batch(2, 1);
		
		b.addTuple(null);
	}
	
}
