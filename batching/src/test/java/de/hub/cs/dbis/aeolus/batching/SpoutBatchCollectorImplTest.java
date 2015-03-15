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

import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.TopologyContext;





/**
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class SpoutBatchCollectorImplTest {
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testBatchEmit() throws IllegalArgumentException, IllegalAccessException {
		ISpoutOutputCollector col = mock(ISpoutOutputCollector.class);
		SpoutBatchCollector collector = mock(SpoutBatchCollector.class);
		PowerMockito.field(SpoutBatchCollector.class, "collector").set(collector, col);
		
		SpoutBatchCollectorImpl collectorImpl = new SpoutBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		
		String streamId = new String();
		Batch batch = mock(Batch.class);
		Object messageId = mock(Object.class);
		
		collectorImpl.batchEmit(streamId, null, batch, messageId);
		
		verify(col).emit(streamId, (List)batch, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Test(expected = AssertionError.class)
	public void testBatchEmitAnchors() {
		SpoutBatchCollector collector = mock(SpoutBatchCollector.class);
		SpoutBatchCollectorImpl collectorImpl = new SpoutBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		collectorImpl.batchEmit(null, mock(Collection.class), null, null);
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void batchEmitDirect() throws IllegalArgumentException, IllegalAccessException {
		ISpoutOutputCollector col = mock(ISpoutOutputCollector.class);
		SpoutBatchCollector collector = mock(SpoutBatchCollector.class);
		PowerMockito.field(SpoutBatchCollector.class, "collector").set(collector, col);
		
		SpoutBatchCollectorImpl collectorImpl = new SpoutBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		
		int taskId = 0;
		String streamId = new String();
		Batch batch = mock(Batch.class);
		Object messageId = mock(Object.class);
		
		collectorImpl.batchEmitDirect(taskId, streamId, null, batch, messageId);
		
		verify(col).emitDirect(taskId, streamId, (List)batch, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Test(expected = AssertionError.class)
	public void testBatchEmitDirectAnchors() {
		SpoutBatchCollector collector = mock(SpoutBatchCollector.class);
		SpoutBatchCollectorImpl collectorImpl = new SpoutBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		collectorImpl.batchEmitDirect(0, null, mock(Collection.class), null, null);
	}
}
