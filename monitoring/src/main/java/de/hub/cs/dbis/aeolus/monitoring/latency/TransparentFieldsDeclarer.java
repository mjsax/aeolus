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
package de.hub.cs.dbis.aeolus.monitoring.latency;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;





/**
 * {@link TransparentFieldsDeclarer} allows to access all declared output streams and their schemas.
 * 
 * @author mjsax
 */
class TransparentFieldsDeclarer implements OutputFieldsDeclarer {
	/** The original schemas for all declared streams. */
	HashMap<String, List<String>> declaredStreams = new HashMap<String, List<String>>();
	/** All streams that are direct streams. */
	HashSet<String> directStreams = new HashSet<String>();
	
	
	
	@Override
	public void declare(Fields fields) {
		this.declare(false, fields);
	}
	
	@Override
	public void declare(boolean direct, Fields fields) {
		this.declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
		
	}
	
	@Override
	public void declareStream(String streamId, Fields fields) {
		this.declareStream(streamId, false, fields);
	}
	
	@Override
	public void declareStream(String streamId, boolean direct, Fields fields) {
		this.declaredStreams.put(streamId, fields.toList());
		if(direct) {
			this.directStreams.add(streamId);
		}
	}
	
}
