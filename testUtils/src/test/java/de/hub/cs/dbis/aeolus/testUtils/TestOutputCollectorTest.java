package de.hub.cs.dbis.aeolus.testUtils;

/*
 * #%L
 * testUtils
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

import static org.mockito.Mockito.mock;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class TestOutputCollectorTest {
	
	@Test
	public void testEmit() {
		TestOutputCollector collector = new TestOutputCollector();
		
		Values tuple = mock(Values.class);
		collector.emit(null, null, tuple);
		
		LinkedList<Values> result = new LinkedList<Values>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.output.get(null));
	}
	
	@Test
	public void testAck() {
		TestOutputCollector collector = new TestOutputCollector();
		
		Tuple tuple = mock(Tuple.class);
		collector.ack(tuple);
		
		LinkedList<Tuple> result = new LinkedList<Tuple>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.acked);
	}
	
	@Test
	public void testFail() {
		TestOutputCollector collector = new TestOutputCollector();
		
		Tuple tuple = mock(Tuple.class);
		collector.fail(tuple);
		
		LinkedList<Tuple> result = new LinkedList<Tuple>();
		result.add(tuple);
		
		Assert.assertEquals(result, collector.failed);
	}
	
}
