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
package de.hub.cs.dbis.aeolus.testUtils;

import java.util.ArrayList;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;





/**
 * @author Matthias J. Sax
 */
public class TestDeclarer implements OutputFieldsDeclarer {
	public ArrayList<Fields> schemaBuffer = new ArrayList<Fields>();
	public ArrayList<String> streamIdBuffer = new ArrayList<String>();
	public ArrayList<Boolean> directBuffer = new ArrayList<Boolean>();
	
	@Override
	public void declare(Fields fields) {
		this.schemaBuffer.add(fields);
		this.streamIdBuffer.add(null);
		this.directBuffer.add(new Boolean(false));
	}
	
	@Override
	public void declare(boolean direct, Fields fields) {
		this.schemaBuffer.add(fields);
		this.streamIdBuffer.add(null);
		this.directBuffer.add(new Boolean(direct));
	}
	
	@Override
	public void declareStream(String streamId, Fields fields) {
		this.schemaBuffer.add(fields);
		this.streamIdBuffer.add(streamId);
		this.directBuffer.add(new Boolean(false));
	}
	
	@Override
	public void declareStream(String streamId, boolean direct, Fields fields) {
		this.schemaBuffer.add(fields);
		this.streamIdBuffer.add(streamId);
		this.directBuffer.add(new Boolean(direct));
	}
	
}
