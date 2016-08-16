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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import backtype.storm.topology.IComponent;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;





/**
 * {@link TimestampAttributeAppender} appends a "create timestamp" attribute to schema of all declared streams.
 * 
 * @author mjsax
 */
final class TimestampAttributeAppender {
	/** The name of the additional timestamp attribute. */
	final static String CREATE_TS_FIELD_NAME = "aeouls::create-ts";
	
	
	
	static void addCreateTimestampAttributeToSchemas(IComponent spoutOrBolt, OutputFieldsDeclarer declarer) {
		TransparentFieldsDeclarer transparentDeclarer = new TransparentFieldsDeclarer();
		spoutOrBolt.declareOutputFields(transparentDeclarer);
		
		for(Entry<String, List<String>> declaredStream : transparentDeclarer.declaredStreams.entrySet()) {
			String streamId = declaredStream.getKey();
			boolean direct = transparentDeclarer.directStreams.contains(streamId);
			ArrayList<String> fieldsWithID = new ArrayList<String>(declaredStream.getValue());
			
			if(!streamId.startsWith("aeolus::")) {
				fieldsWithID.add(CREATE_TS_FIELD_NAME);
			}
			
			declarer.declareStream(streamId, direct, new Fields(fieldsWithID));
		}
	}
	
}
