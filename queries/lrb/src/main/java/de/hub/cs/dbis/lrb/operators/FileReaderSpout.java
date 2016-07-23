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
package de.hub.cs.dbis.lrb.operators;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import de.hub.cs.dbis.aeolus.spouts.AbstractOrderedFileInputSpout;
import de.hub.cs.dbis.aeolus.utils.TimestampMerger;





/**
 * {@link FileReaderSpout} reads LRB stream data from multiple files. See {@link AbstractOrderedFileInputSpout} for
 * configuring information of input files. Per default, a single file named {@code xway} is used.<br/>
 * <br />
 * Each file is expected to contain data of a single express way, including position records, and historical query
 * request (ie, account balance, daily expenditure, and travel time query request). Each line must contain a single
 * record and all records in each file must be in ascending timestamp order.<br />
 * <br />
 * <strong>Expected file format:</strong>{@code <Type,Time,remaining-attributes>}<br />
 * where {@code Type} specifies the record type (valid values are 0, 2, 3, or 4) and {@code Time} is the timestamp
 * attribute of the record. The number of remaining attributes depends on the record type. The type attribute and the
 * remaining attributes are ignored while parsing an input record.
 * 
 * @author mjsax
 */
public class FileReaderSpout extends AbstractOrderedFileInputSpout {
	private final static long serialVersionUID = 8536833427236986702L;
	
	/** The prefix of all input file names. */
	private final String defaultPrefix = "xway";
	
	/** The output collector to be used. */
	private SpoutOutputCollector collector;
	
	
	
	/**
	 * Instantiates a new {@link FileReaderSpout} that emits to the default output stream.
	 */
	public FileReaderSpout() {
		super();
	}
	
	/**
	 * Instantiates a new {@link FileReaderSpout} that emits to the specified output stream.
	 * 
	 * @param streamID
	 *            The name of the used output stream.
	 */
	public FileReaderSpout(String streamID) {
		super(streamID);
	}
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") SpoutOutputCollector collector) {
		@SuppressWarnings("unchecked")
		HashMap<Object, Object> newConf = new HashMap<Object, Object>(conf);
		if(newConf.get(INPUT_FILE_NAME) == null) {
			newConf.put(INPUT_FILE_NAME, this.defaultPrefix);
		}
		super.open(newConf, context, collector);
		
		this.collector = collector;
	}
	
	/**
	 * {@inheritDoc} <br/>
	 * <br/>
	 * Each {@code line} is expected to be in CSV format having the timestamp attribute at the second position.
	 */
	@Override
	protected long extractTimestamp(String line) throws ParseException {
		int p1 = line.indexOf(",");
		int p2 = line.indexOf(",", p1 + 1);
		try {
			return Long.parseLong(line.substring(p1 + 1, p2));
		} catch(NumberFormatException e) {
			throw new ParseException(e.getMessage(), p1);
		} catch(IndexOutOfBoundsException e) {
			if(p1 < 0) {
				throw new ParseException("Input string has wrong format. Missing ','.", p1);
			} else {
				assert (p2 < 0);
				throw new ParseException("Input string has wrong format. Missing second ','.", p2);
			}
			
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		super.declareOutputFields(declarer);
		declarer.declareStream(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
	}
	
	@Override
	public void activate() {/* empty */}
	
	@Override
	public void deactivate() {
		this.collector.emit(TimestampMerger.FLUSH_STREAM_ID, new Values((Object)null));
	}
	
	@Override
	public void ack(Object msgId) {/* empty */}
	
	@Override
	public void fail(Object msgId) {/* empty */}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
