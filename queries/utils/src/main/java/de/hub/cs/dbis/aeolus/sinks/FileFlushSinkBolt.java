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
package de.hub.cs.dbis.aeolus.sinks;

import backtype.storm.tuple.Tuple;





/**
 * {@link FileFlushSinkBolt} flushes the written data to disc after each received tuple. I can be used for low
 * throughput streams to guarantee, that all tuples are written to disc (the call to {@link #cleanup()} is not
 * guaranteed by Storm.
 * 
 * @author Matthias J. Sax
 */
public class FileFlushSinkBolt extends FileSinkBolt {
	private static final long serialVersionUID = -2896873750026892533L;
	
	
	
	public FileFlushSinkBolt(String filename) {
		super(filename);
	}
	
	
	
	/**
	 * Writes (and flushed) the given tuple to disc.
	 * 
	 * @param input
	 *            The input tuple to be processed.
	 */
	@Override
	public void execute(Tuple input) {
		super.executeAndFlush(input);
	}
	
}
