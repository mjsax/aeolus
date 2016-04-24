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
package de.hub.cs.dbis.aeolus.batching;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import de.hub.cs.dbis.aeolus.batching.api.BoltOutputBatcher;
import de.hub.cs.dbis.aeolus.batching.api.SpoutOutputBatcher;





/**
 * {@link BatchingOutputFieldsDeclarer} is used by {@link SpoutOutputBatcher} and {@link BoltOutputBatcher} to declare
 * an additional <it>direct</it> output stream for each user declared <it>non</it>-direct output stream.
 * 
 * @author Matthias J. Sax
 */
public class BatchingOutputFieldsDeclarer implements OutputFieldsDeclarer {
	/**
	 * Stream-ID prefix for automatically declared direct output streams.
	 */
	public final static String STREAM_PREFIX = "aeolus::";
	
	/**
	 * The original declarer.
	 */
	private final OutputFieldsDeclarer declarer;
	
	
	
	/**
	 * Instantiates a new {@link BatchingOutputFieldsDeclarer} using the provided declarer object.
	 * 
	 * @param declarer
	 *            The original declarer object.
	 */
	public BatchingOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
		this.declarer = declarer;
	}
	
	
	
	@Override
	public void declare(Fields schema) {
		this.declareStream(Utils.DEFAULT_STREAM_ID, false, schema);
	}
	
	@Override
	public void declare(boolean direct, Fields schema) {
		this.declareStream(Utils.DEFAULT_STREAM_ID, direct, schema);
	}
	
	@Override
	public void declareStream(String streamId, Fields schema) {
		this.declareStream(streamId, false, schema);
	}
	
	@Override
	public void declareStream(String streamId, boolean direct, Fields schema) {
		this.declarer.declareStream(streamId, direct, schema);
		if(!direct) {
			this.declarer.declareStream(STREAM_PREFIX + streamId, true, schema);
		}
	}
	
}
