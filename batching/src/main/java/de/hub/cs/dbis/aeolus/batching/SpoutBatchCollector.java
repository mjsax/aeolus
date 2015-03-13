package de.hub.cs.dbis.aeolus.batching;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;





/**
 * {@link SpoutBatchCollector} is used by {@link SpoutOutputBatcher} to capture all calls to the original provided
 * {@link SpoutOutputCollector}. It used {@link AbstractBatchCollector} to buffer all emitted tuples in batches.
 * 
 * @author Matthias J. Sax
 */
class SpoutBatchCollector extends SpoutOutputCollector {
	private final static Logger LOGGER = LoggerFactory.getLogger(SpoutBatchCollector.class);
	
	/**
	 * TODO
	 */
	private final ISpoutOutputCollector collector;
	/**
	 * TODO
	 */
	private final AbstractBatchCollector batcher;
	
	
	
	/**
	 * TODO
	 * 
	 * @param context
	 * @param collector
	 * @param batchSize
	 */
	SpoutBatchCollector(TopologyContext context, ISpoutOutputCollector collector, int batchSize) {
		super(collector);
		LOGGER.trace("batchSize: {}", batchSize);
		
		this.collector = collector;
		this.batcher = new AbstractBatchCollector(context, batchSize) {
			@Override
			protected List<Integer> batchEmit(String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
				assert (anchors == null);
				LOGGER.trace("streamId: {}; batch: {}; messageId: {}", streamId, batch, messageId);
				return SpoutBatchCollector.this.collector.emit(streamId, (List)batch, messageId);
			}
			
			@Override
			protected void batchEmitDirect(int taskId, String streamId, Collection<Tuple> anchors, Batch batch, Object messageId) {
				assert (anchors == null);
				LOGGER.trace("taskId: {}; streamId: {}; batch: {}; messageId: {}", taskId, streamId, batch, messageId);
				SpoutBatchCollector.this.collector.emitDirect(taskId, streamId, (List)batch, messageId);
			}
		};
	}
	
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		LOGGER.trace("streamId: {}; tuple: {}; messageId: {}", streamId, tuple, messageId);
		return this.batcher.tupleEmit(streamId, null, tuple, messageId);
	}
	
	// need to copy and override to redirect call to SpoutBatchCollector.emit(String streamId, List<Object> tuple,
	// Object messageId)
	@Override
	public List<Integer> emit(List<Object> tuple, Object messageId) {
		return this.emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}
	
	// need to copy and override to redirect call to SpoutBatchCollector.emit(List<Object> tuple, Object messageId)
	@Override
	public List<Integer> emit(List<Object> tuple) {
		return this.emit(tuple, null);
	}
	
	// need to copy and override to redirect call to SpoutBatchCollector.emit(String streamId, List<Object> tuple,
	// Object messageId)
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		return this.emit(streamId, tuple, null);
	}
	
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		LOGGER.trace("taskId: {}; streamId: {}; tuple: {}; messageId: {}", taskId, streamId, tuple, messageId);
		this.batcher.tupleEmitDirect(taskId, streamId, null, tuple, messageId);
	}
	
	// need to copy and override to redirect call to SpoutBatchCollector.emitDirect(int taskId, String streamId,
	// List<Object> tuple, Object messageId)
	@Override
	public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
		this.emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}
	
	// need to copy and override to redirect call to SpoutBatchCollector.emitDirect(int taskId, String streamId,
	// List<Object> tuple, Object messageId)
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		this.emitDirect(taskId, streamId, tuple, null);
	}
	
	// need to copy and override to redirect call to SpoutBatchCollector.emitDirect(int taskId, String streamId,
	// List<Object> tuple, Object messageId)
	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		this.emitDirect(taskId, tuple, null);
	}
	
}
