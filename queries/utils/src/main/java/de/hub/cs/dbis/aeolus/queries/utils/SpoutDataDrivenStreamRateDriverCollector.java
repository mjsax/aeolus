package de.hub.cs.dbis.aeolus.queries.utils;

import java.util.List;

import backtype.storm.spout.SpoutOutputCollector;





/**
 * {@link SpoutDataDrivenStreamRateDriverCollector} forward all calls to the wrapped {@link SpoutOutputCollector}.
 * Before forwarding, it extract the timestamp attribute value from the currently emitted tuple. The timestamp attribute
 * is expected to be of type {@link Number} and is stored as {@code long}.
 * 
 * @author Matthias J. Sax
 */
public class SpoutDataDrivenStreamRateDriverCollector<T extends Number> extends SpoutOutputCollector {
	/**
	 * The original output collector.
	 */
	private final SpoutOutputCollector collector;
	/**
	 * The index of the timestamp attribute.
	 */
	private final int tsIndex;
	/**
	 * The timestamp of the last emitted tuple.
	 */
	long timestampLastTuple;
	
	
	
	/**
	 * Instantiates a new {@link SpoutDataDrivenStreamRateDriverCollector} for the given timestamp attribute index.
	 * 
	 * @param collector
	 *            The collector to be wrapped
	 * @param tsIndex
	 *            The index of the timestamp attribute.
	 */
	public SpoutDataDrivenStreamRateDriverCollector(SpoutOutputCollector collector, int tsIndex) {
		super(collector);
		assert (collector != null);
		assert (tsIndex >= 0);
		
		this.collector = collector;
		this.tsIndex = tsIndex;
	}
	
	
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		return this.collector.emit(streamId, tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(List<Object> tuple, Object messageId) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		return this.collector.emit(tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(List<Object> tuple) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		return this.collector.emit(tuple);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		return this.collector.emit(streamId, tuple);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		this.collector.emitDirect(taskId, streamId, tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		this.collector.emitDirect(taskId, tuple, messageId);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		this.collector.emitDirect(taskId, streamId, tuple);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		this.timestampLastTuple = ((T)tuple.get(this.tsIndex)).longValue();
		this.collector.emitDirect(taskId, tuple);
	}
	
	@Override
	public void reportError(Throwable error) {
		this.collector.reportError(error);
	}
	
}
