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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BatchOutputCollector.class)
public class BatchOutputCollectorTest {
	private static BoltBatchCollectorImpl collectorMock;
	
	private static int taskId;
	private static String streamId = new String();
	private static Tuple anchor = mock(Tuple.class);
	private static List<Tuple> anchors = new LinkedList<Tuple>();
	private static Values tuple = new Values();
	
	
	
	@BeforeClass
	public static void prepareTestStatic() {
		anchors.add(anchor);
		
	}
	
	@Before
	public void prepareTest() throws Exception {
		long seed = System.currentTimeMillis();
		Random r = new Random(seed);
		System.out.println("Test seed: " + seed);
		
		taskId = r.nextInt(10);
		
		collectorMock = mock(BoltBatchCollectorImpl.class);
		PowerMockito.whenNew(BoltBatchCollectorImpl.class).withAnyArguments().thenReturn(collectorMock);
	}
	
	
	
	@Test
	public void testEmitFull() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emit(streamId, anchors, tuple);
		verify(collectorMock).tupleEmit(streamId, anchors, tuple, null);
	}
	
	@Test
	public void testEmitStreamIdAnchorTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emit(streamId, anchor, tuple);
		verify(collectorMock).tupleEmit(streamId, anchors, tuple, null);
	}
	
	@Test
	public void testEmitStreamIdTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emit(streamId, tuple);
		verify(collectorMock).tupleEmit(streamId, null, tuple, null);
	}
	
	@Test
	public void testEmitAnchorsTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emit(anchors, tuple);
		verify(collectorMock).tupleEmit(Utils.DEFAULT_STREAM_ID, anchors, tuple, null);
	}
	
	@Test
	public void testEmitAnchorTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emit(anchor, tuple);
		verify(collectorMock).tupleEmit(Utils.DEFAULT_STREAM_ID, anchors, tuple, null);
	}
	
	@Test
	public void testEmitTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emit(tuple);
		verify(collectorMock).tupleEmit(Utils.DEFAULT_STREAM_ID, null, tuple, null);
	}
	
	@Test
	public void testEmitDirectFull() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emitDirect(taskId, streamId, anchors, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, streamId, anchors, tuple, null);
	}
	
	@Test
	public void testEmitDirectStreamIdAnchorTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emitDirect(taskId, streamId, anchor, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, streamId, anchors, tuple, null);
	}
	
	@Test
	public void testEmitDirectStreamIdTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emitDirect(taskId, streamId, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, streamId, null, tuple, null);
	}
	
	@Test
	public void testEmitDirectAnchorsTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emitDirect(taskId, anchors, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchors, tuple, null);
	}
	
	@Test
	public void testEmitDirectAnchorTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emitDirect(taskId, anchor, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, Utils.DEFAULT_STREAM_ID, anchors, tuple, null);
	}
	
	@Test
	public void testEmitDirectTuple() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.emitDirect(taskId, tuple);
		verify(collectorMock).tupleEmitDirect(taskId, Utils.DEFAULT_STREAM_ID, null, tuple, null);
	}
	
	@Test
	public void testFlush() {
		BatchOutputCollector collector = new BatchOutputCollector(null, null, 0);
		collector.flush();
		verify(collectorMock).flush();
	}
	
}
