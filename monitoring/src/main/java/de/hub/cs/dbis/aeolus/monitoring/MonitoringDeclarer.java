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
package de.hub.cs.dbis.aeolus.monitoring;

import java.util.HashSet;
import java.util.Set;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;





class MonitoringDeclarer implements OutputFieldsDeclarer {
	Set<String> declaredStreams = new HashSet<String>();
	
	@Override
	public void declare(Fields fields) {
		this.declaredStreams.add(Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public void declare(boolean direct, Fields fields) {
		this.declaredStreams.add(Utils.DEFAULT_STREAM_ID);
	}
	
	@Override
	public void declareStream(String streamId, Fields fields) {
		this.declaredStreams.add(streamId);
	}
	
	@Override
	public void declareStream(String streamId, boolean direct, Fields fields) {
		this.declaredStreams.add(streamId);
	}
}
