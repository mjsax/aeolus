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
package de.hub.cs.dbis.aeolus.lrb.testutils;

import de.hub.cs.dbis.aeolus.testUtils.AbstractBoltTest;
import de.hub.cs.dbis.aeolus.testUtils.TestOutputCollector;
import de.hub.cs.dbis.lrb.types.LavTuple;
import java.util.HashSet;
import java.util.List;
import static org.junit.Assert.assertEquals;





/**
 * 
 * @author richter
 */
public class AbstractLRBTest extends AbstractBoltTest {
	
	/**
	 * Compares the ordered sequence (aka list) of tuples {@code expectedResult} and {@code result} by ignoring their
	 * ordering, i.e. this method will pass iff a sequence exists which contains all elements of {@code result} and
	 * {@code expectedResult} and no other elements. If the method won't pass it will throw an {@link AssertionError}.
	 * 
	 * The method signature furthermore enforces the assertion that the acked tuples in {@code collected.acked} are
	 * equals {@code executeCounter}.
	 * 
	 * Note: items will be removed from {@code expectedResult} and {@code result} as they're throw-away objects and it's
	 * even cheaper to remove the first item of a list then to build iterators and use them.
	 * 
	 * @param expectedResult
	 * @param result
	 * @param executeCounter
	 * @param collector
	 */
	protected static void assertLavTupleInputEquals(List<LavTuple> expectedResult, List<List<Object>> result, int executeCounter, TestOutputCollector collector) {
		while(expectedResult.size() > 0) {
			LavTuple t = expectedResult.remove(0);
			HashSet<List<Object>> expectedResultSet = new HashSet<List<Object>>();
			expectedResultSet.add(t);
			
			HashSet<List<Object>> resultSet = new HashSet<List<Object>>();
			resultSet.add(result.remove(0));
			
			while(expectedResult.size() > 0) {
				LavTuple t2 = expectedResult.get(0);
				if(t2.getMinuteNumber().shortValue() == t.getMinuteNumber().shortValue()) {
					expectedResultSet.add(expectedResult.remove(0));
					resultSet.add(result.remove(0));
				} else {
					break;
				}
			}
			
			assertEquals(expectedResultSet, resultSet);
		}
		assertEquals(0, result.size());
		assertEquals(executeCounter, collector.acked.size());
	}
}
