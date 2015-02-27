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
		
		Assert.assertEquals(4, declarer.schema.size());
		Assert.assertEquals(4, declarer.streamId.size());
		Assert.assertEquals(4, declarer.direct.size());
		
		Assert.assertSame(schema[0], declarer.schema.get(0));
		Assert.assertEquals(null, declarer.streamId.get(0));
		Assert.assertEquals(new Boolean(false), declarer.direct.get(0));
		
		Assert.assertSame(schema[1], declarer.schema.get(1));
		Assert.assertEquals(null, declarer.streamId.get(1));
		Assert.assertEquals(new Boolean(true), declarer.direct.get(1));
		
		Assert.assertSame(schema[2], declarer.schema.get(2));
		Assert.assertEquals(stream[0], declarer.streamId.get(2));
		Assert.assertEquals(new Boolean(false), declarer.direct.get(2));
		
		Assert.assertSame(schema[3], declarer.schema.get(3));
		Assert.assertEquals(stream[1], declarer.streamId.get(3));
		Assert.assertEquals(new Boolean(true), declarer.direct.get(3));
	}
}
