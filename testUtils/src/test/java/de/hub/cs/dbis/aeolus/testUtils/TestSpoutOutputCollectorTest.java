/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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
package de.hub.cs.dbis.aeolus.testUtils;

import static org.mockito.Mockito.mock;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Values;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class TestSpoutOutputCollectorTest {
	
	@Test
	public void testEmit() {
		TestSpoutOutputCollector collector = new TestSpoutOutputCollector();
		
		Values tuple = mock(Values.class);
		collector.emit(null, tuple, null);
		
		LinkedList<Values> result = new LinkedList<Values>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.output.get(null));
	}
	
}
