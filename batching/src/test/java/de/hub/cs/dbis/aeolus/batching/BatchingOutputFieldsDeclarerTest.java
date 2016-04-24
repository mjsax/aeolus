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
package de.hub.cs.dbis.aeolus.batching;

import org.junit.Assert;
import org.junit.Test;

import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.testUtils.TestDeclarer;





/**
 * @author Matthias J. Sax
 */
public class BatchingOutputFieldsDeclarerTest {
	
	@Test
	public void testDelareSimple() {
		TestDeclarer testDeclarer = new TestDeclarer();
		BatchingOutputFieldsDeclarer declarer = new BatchingOutputFieldsDeclarer(testDeclarer);
		
		declarer.declare(new Fields("dummy"));
		
		Assert.assertFalse(testDeclarer.directBuffer.get(0).booleanValue());
		Assert.assertTrue(testDeclarer.directBuffer.get(1).booleanValue());
		Assert.assertEquals(new Fields("dummy").toList(), testDeclarer.schemaBuffer.get(0).toList());
		Assert.assertEquals(new Fields("dummy").toList(), testDeclarer.schemaBuffer.get(1).toList());
		Assert.assertEquals(Utils.DEFAULT_STREAM_ID, testDeclarer.streamIdBuffer.get(0));
		Assert.assertEquals(BatchingOutputFieldsDeclarer.STREAM_PREFIX + Utils.DEFAULT_STREAM_ID,
			testDeclarer.streamIdBuffer.get(1));
	}
	
	@Test
	public void testDelareDirect() {
		TestDeclarer testDeclarer = new TestDeclarer();
		BatchingOutputFieldsDeclarer declarer = new BatchingOutputFieldsDeclarer(testDeclarer);
		
		declarer.declare(true, new Fields("dummy"));
		
		Assert.assertEquals(1, testDeclarer.directBuffer.size());
		Assert.assertTrue(testDeclarer.directBuffer.get(0).booleanValue());
		Assert.assertEquals(new Fields("dummy").toList(), testDeclarer.schemaBuffer.get(0).toList());
		Assert.assertEquals(1, testDeclarer.streamIdBuffer.size());
		Assert.assertEquals(Utils.DEFAULT_STREAM_ID, testDeclarer.streamIdBuffer.get(0));
	}
	
	@Test
	public void testDelareStream() {
		TestDeclarer testDeclarer = new TestDeclarer();
		BatchingOutputFieldsDeclarer declarer = new BatchingOutputFieldsDeclarer(testDeclarer);
		
		declarer.declareStream("streamId", new Fields("dummy"));
		
		Assert.assertFalse(testDeclarer.directBuffer.get(0).booleanValue());
		Assert.assertTrue(testDeclarer.directBuffer.get(1).booleanValue());
		Assert.assertEquals(new Fields("dummy").toList(), testDeclarer.schemaBuffer.get(0).toList());
		Assert.assertEquals(new Fields("dummy").toList(), testDeclarer.schemaBuffer.get(1).toList());
		Assert.assertEquals("streamId", testDeclarer.streamIdBuffer.get(0));
		Assert
			.assertEquals(BatchingOutputFieldsDeclarer.STREAM_PREFIX + "streamId", testDeclarer.streamIdBuffer.get(1));
	}
	
	@Test
	public void testDelareFull() {
		TestDeclarer testDeclarer = new TestDeclarer();
		BatchingOutputFieldsDeclarer declarer = new BatchingOutputFieldsDeclarer(testDeclarer);
		
		declarer.declareStream("streamId", true, new Fields("dummy"));
		
		Assert.assertEquals(1, testDeclarer.directBuffer.size());
		Assert.assertTrue(testDeclarer.directBuffer.get(0).booleanValue());
		Assert.assertEquals(new Fields("dummy").toList(), testDeclarer.schemaBuffer.get(0).toList());
		Assert.assertEquals(1, testDeclarer.streamIdBuffer.size());
		Assert.assertEquals("streamId", testDeclarer.streamIdBuffer.get(0));
	}
	
}
