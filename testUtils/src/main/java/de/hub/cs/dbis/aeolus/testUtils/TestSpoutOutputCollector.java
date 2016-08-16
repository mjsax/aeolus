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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import backtype.storm.spout.ISpoutOutputCollector;





/**
 * @author mjsax
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
	
	// @Override
	// public long getPendingCount() {
	// return 0;
	// }
	//
}
