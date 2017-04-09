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

import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;





/**
 * @author mjsax
 */
@RunWith(PowerMockRunner.class)
public class BoltBatchCollectorImplTest {
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testBatchEmit() throws IllegalArgumentException, IllegalAccessException {
		IOutputCollector col = mock(IOutputCollector.class);
		BatchOutputCollector collector = mock(BatchOutputCollector.class);
		PowerMockito.field(BatchOutputCollector.class, "collector").set(collector, col);
		
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		
		String streamId = new String();
		Collection<Tuple> anchors = mock(Collection.class);
		Batch batch = mock(Batch.class);
		
		collectorImpl.doEmit(streamId, anchors, batch, null);
		
		verify(col).emit(streamId, anchors, (List)batch);
	}
	
	@Test(expected = AssertionError.class)
	public void testBatchEmitAnchors() {
		BatchOutputCollector collector = mock(BatchOutputCollector.class);
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		collectorImpl.doEmit(null, null, null, mock(Object.class));
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void batchEmitDirect() throws IllegalArgumentException, IllegalAccessException {
		IOutputCollector col = mock(IOutputCollector.class);
		BatchOutputCollector collector = mock(BatchOutputCollector.class);
		PowerMockito.field(BatchOutputCollector.class, "collector").set(collector, col);
		
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		
		int taskId = 0;
		String streamId = new String();
		Collection<Tuple> anchors = mock(Collection.class);
		Batch batch = mock(Batch.class);
		
		collectorImpl.doEmitDirect(taskId, streamId, anchors, batch, null);
		
		verify(col).emitDirect(taskId, streamId, anchors, (List)batch);
	}
	
	@Test(expected = AssertionError.class)
	public void testBatchEmitDirectAnchors() {
		BatchOutputCollector collector = mock(BatchOutputCollector.class);
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		collectorImpl.doEmitDirect(0, null, null, null, mock(Object.class));
	}
	
}
