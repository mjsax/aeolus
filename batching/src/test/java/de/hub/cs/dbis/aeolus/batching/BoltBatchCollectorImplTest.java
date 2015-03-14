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
 * @author Matthias J. Sax
 */
@RunWith(PowerMockRunner.class)
public class BoltBatchCollectorImplTest {
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testBatchEmit() throws IllegalArgumentException, IllegalAccessException {
		IOutputCollector col = mock(IOutputCollector.class);
		BoltBatchCollector collector = mock(BoltBatchCollector.class);
		PowerMockito.field(BoltBatchCollector.class, "collector").set(collector, col);
		
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		
		String streamId = new String();
		Collection<Tuple> anchors = mock(Collection.class);
		Batch batch = mock(Batch.class);
		
		collectorImpl.batchEmit(streamId, anchors, batch, null);
		
		verify(col).emit(streamId, anchors, (List)batch);
	}
	
	@Test(expected = AssertionError.class)
	public void testBatchEmitAnchors() {
		BoltBatchCollector collector = mock(BoltBatchCollector.class);
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		collectorImpl.batchEmit(null, null, null, mock(Object.class));
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void batchEmitDirect() throws IllegalArgumentException, IllegalAccessException {
		IOutputCollector col = mock(IOutputCollector.class);
		BoltBatchCollector collector = mock(BoltBatchCollector.class);
		PowerMockito.field(BoltBatchCollector.class, "collector").set(collector, col);
		
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		
		int taskId = 0;
		String streamId = new String();
		Collection<Tuple> anchors = mock(Collection.class);
		Batch batch = mock(Batch.class);
		
		collectorImpl.batchEmitDirect(taskId, streamId, anchors, batch, null);
		
		verify(col).emitDirect(taskId, streamId, anchors, (List)batch);
	}
	
	@Test(expected = AssertionError.class)
	public void testBatchEmitDirectAnchors() {
		BoltBatchCollector collector = mock(BoltBatchCollector.class);
		BoltBatchCollectorImpl collectorImpl = new BoltBatchCollectorImpl(collector, mock(TopologyContext.class), 0);
		collectorImpl.batchEmitDirect(0, null, null, null, mock(Object.class));
	}
	
}
