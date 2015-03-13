package de.hub.cs.dbis.aeolus.testUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import backtype.storm.spout.ISpoutOutputCollector;





/**
 * @author Matthias J. Sax
 */
public class TestSpoutOutputCollector implements ISpoutOutputCollector {
	public HashMap<String, LinkedList<List<Object>>> output = new HashMap<String, LinkedList<List<Object>>>();
	
	@Override
	public void reportError(Throwable error) {
		// empty
	}
	
	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		LinkedList<List<Object>> stream = this.output.get(streamId);
		if(stream == null) {
			stream = new LinkedList<List<Object>>();
			this.output.put(streamId, stream);
		}
		stream.add(tuple);
		return null;
	}
	
	@Override
	// TODO
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		throw new UnsupportedOperationException("Not implemented yet.");
	}
	
}
