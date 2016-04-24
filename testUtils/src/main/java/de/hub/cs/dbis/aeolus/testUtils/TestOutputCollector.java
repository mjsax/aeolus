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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;





/**
 * @author Matthias J. Sax
 */
public class TestOutputCollector implements IOutputCollector {
	public HashMap<String, List<List<Object>>> output = new HashMap<String, List<List<Object>>>();
	public LinkedList<Tuple> acked = new LinkedList<Tuple>();
	public LinkedList<Tuple> failed = new LinkedList<Tuple>();
	
	
	@Override
	public void reportError(Throwable error) {
		// empty
	}
	
	@Override
	public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		List<List<Object>> streamOutput = this.output.get(streamId);
		if(streamOutput == null) {
			streamOutput = new LinkedList<List<Object>>();
			this.output.put(streamId, streamOutput);
		}
		streamOutput.add(tuple);
		return null;
	}
	
	@Override
	// TODO
	public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
		throw new UnsupportedOperationException("Not implemented yet.");
	}
	
	@Override
	public void ack(Tuple input) {
		this.acked.add(input);
	}
	
	@Override
	public void fail(Tuple input) {
		this.failed.add(input);
	}
	
}
