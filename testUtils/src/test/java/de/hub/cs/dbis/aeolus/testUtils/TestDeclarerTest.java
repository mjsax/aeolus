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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Fields;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class TestDeclarerTest {
	
	@Test
	public void testDeclare() {
		TestDeclarer declarer = new TestDeclarer();
		
		Fields[] schema = new Fields[] {mock(Fields.class), mock(Fields.class), mock(Fields.class), mock(Fields.class)};
		String[] stream = new String[] {"stream1", "stream2"};
		
		declarer.declare(schema[0]);
		declarer.declare(true, schema[1]);
		declarer.declareStream(stream[0], schema[2]);
		declarer.declareStream(stream[1], true, schema[3]);
		
		Assert.assertEquals(4, declarer.schemaBuffer.size());
		Assert.assertEquals(4, declarer.streamIdBuffer.size());
		Assert.assertEquals(4, declarer.directBuffer.size());
		
		Assert.assertSame(schema[0], declarer.schemaBuffer.get(0));
		Assert.assertNull(declarer.streamIdBuffer.get(0));
		Assert.assertFalse(declarer.directBuffer.get(0).booleanValue());
		
		Assert.assertSame(schema[1], declarer.schemaBuffer.get(1));
		Assert.assertNull(declarer.streamIdBuffer.get(1));
		Assert.assertTrue(declarer.directBuffer.get(1).booleanValue());
		
		Assert.assertSame(schema[2], declarer.schemaBuffer.get(2));
		Assert.assertEquals(stream[0], declarer.streamIdBuffer.get(2));
		Assert.assertFalse(declarer.directBuffer.get(2).booleanValue());
		
		Assert.assertSame(schema[3], declarer.schemaBuffer.get(3));
		Assert.assertEquals(stream[1], declarer.streamIdBuffer.get(3));
		Assert.assertTrue(declarer.directBuffer.get(3).booleanValue());
	}
	
}
