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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BatchSpoutOutputCollector.class)
public class BatchSpoutOutputCollectorTest {
	private static SpoutBatchCollectorImpl collectorMock;
	
	private static int taskId;
	private static String streamId = new String();
	private static Values tuple = new Values();
	private static Object messageId;
	
	
	
	@Before
	public void prepareTest() throws Exception {
		long seed = System.currentTimeMillis();
		Random r = new Random(seed);
		System.out.println("Test seed: " + seed);
		
		taskId = r.nextInt(10);
		messageId = new Integer(r.nextInt());
		
		collectorMock = mock(SpoutBatchCollectorImpl.class);
		PowerMockito.whenNew(SpoutBatchCollectorImpl.class).withAnyArguments().thenReturn(collectorMock);
	}
	
	
	
	@Test
	public void testEmitFull() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emit(streamId, tuple, messageId);
		verify(collectorMock).tupleEmit(streamId, null, tuple, messageId);
	}
	
	@Test
	public void testEmitTupleMessageId() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emit(tuple, messageId);
		verify(collectorMock).tupleEmit(Utils.DEFAULT_STREAM_ID, null, tuple, messageId);
	}
	
	@Test
	public void testEmitTuple() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emit(tuple);
		verify(collectorMock).tupleEmit(Utils.DEFAULT_STREAM_ID, null, tuple, null);
	}
	
	@Test
	public void testEmitStreamIdTuple() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emit(streamId, tuple);
		verify(collectorMock).tupleEmit(streamId, null, tuple, null);
	}
	
	@Test
	public void testEmitDirectFull() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emitDirect(taskId, streamId, tuple, messageId);
		verify(collectorMock).tupleEmitDirect(taskId, streamId, null, tuple, messageId);
	}
	
	@Test
	public void testEmitDirectTupleMessageId() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emitDirect(taskId, tuple, messageId);
		verify(collectorMock).tupleEmitDirect(taskId, Utils.DEFAULT_STREAM_ID, null, tuple, messageId);
	}
	
	@Test
	public void testEmitDirectStreamIdTuple() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emitDirect(taskId, streamId, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, streamId, null, tuple, null);
	}
	
	@Test
	public void testEmitDirectTuple() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.emitDirect(taskId, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, Utils.DEFAULT_STREAM_ID, null, tuple, null);
	}
	
	@Test
	public void testFlush() {
		BatchSpoutOutputCollector collector = new BatchSpoutOutputCollector(null, null, 0);
		collector.flush();
		verify(collectorMock).flush();
	}
	
}
