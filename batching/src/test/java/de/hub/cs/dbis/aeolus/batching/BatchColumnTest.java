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





/**
 * @author Matthias J. Sax
 */
public class BatchColumnTest {
	private long seed;
	private Random r;
	
	
	@Before
	public void prepare() {
		this.seed = System.currentTimeMillis();
		this.r = new Random(this.seed);
		System.out.println("Test seed: " + this.seed);
	}
	
	@Test
	public void testHashCode() {
		final int batchSize = 1 + this.r.nextInt(20);
		BatchColumn bc = new BatchColumn(batchSize);
		
		int numberOfTuples;
		switch(this.r.nextInt(2)) {
		case 0:
			numberOfTuples = batchSize;
			break;
		default:
			numberOfTuples = 1 + this.r.nextInt(batchSize);
		}
		
		Double first = new Double(this.r.nextDouble() + this.r.nextLong());
		bc.add(first);
		
		for(int i = 1; i < numberOfTuples; ++i) {
			bc.add(new Double(this.r.nextDouble() + this.r.nextLong()));
		}
		
		Assert.assertEquals(first.hashCode(), bc.hashCode());
	}
}
