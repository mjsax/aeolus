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
package de.hub.cs.dbis.aeolus.spouts;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Random;

import de.hub.cs.dbis.aeolus.spouts.AbstractOrderedInputSpout;
import backtype.storm.tuple.Values;





class TestOrderedInputSpout extends AbstractOrderedInputSpout<String> {
	private final static long serialVersionUID = -6722924299495546729L;
	
	private final List<Deque<String>> data;
	private final Random rr;
	
	Map<Values, List<Integer>> emitted;
	
	TestOrderedInputSpout(List<Deque<String>> data, Random rr) {
		this.data = data;
		this.rr = rr;
	}
	
	public TestOrderedInputSpout(List<Deque<String>> data, Random rr, String streamID) {
		super(streamID);
		this.data = data;
		this.rr = rr;
	}
	
	
	@Override
	public void nextTuple() {
		int index = this.rr.nextInt(this.data.size());
		int old = index;
		while(this.data.get(index).size() == 0) {
			index = (index + 1) % this.data.size();
			if(index == old) {
				this.emitted = super.emitNextTuple(null, null, null);
				return;
			}
		}
		String line = this.data.get(index).removeFirst();
		this.emitted = super.emitNextTuple(new Integer(index), new Long(Long.parseLong(line.trim())), line);
	}
	
	@Override
	public void close() {}
	
	@Override
	public void activate() {}
	
	@Override
	public void deactivate() {}
	
	@Override
	public void ack(Object msgId) {}
	
	@Override
	public void fail(Object msgId) {}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
