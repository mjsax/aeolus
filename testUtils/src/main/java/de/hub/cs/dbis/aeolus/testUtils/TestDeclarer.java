package de.hub.cs.dbis.aeolus.testUtils;

/*
 * #%L
 * testUtils
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */

import java.util.ArrayList;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;





/**
 * @author Matthias J. Sax
 */
public class TestDeclarer implements OutputFieldsDeclarer {
	public ArrayList<Fields> schema = new ArrayList<Fields>();
	public ArrayList<String> streamId = new ArrayList<String>();
	public ArrayList<Boolean> direct = new ArrayList<Boolean>();
	
	@Override
	public void declare(Fields fields) {
		this.schema.add(fields);
		this.streamId.add(null);
		this.direct.add(new Boolean(false));
	}
	
	@Override
	public void declare(boolean direct, Fields fields) {
		this.schema.add(fields);
		this.streamId.add(null);
		this.direct.add(new Boolean(direct));
	}
	
	@Override
	public void declareStream(String streamId, Fields fields) {
		this.schema.add(fields);
		this.streamId.add(streamId);
		this.direct.add(new Boolean(false));
	}
	
	@Override
	public void declareStream(String streamId, boolean direct, Fields fields) {
		this.schema.add(fields);
		this.streamId.add(streamId);
		this.direct.add(new Boolean(direct));
	}
	
}
