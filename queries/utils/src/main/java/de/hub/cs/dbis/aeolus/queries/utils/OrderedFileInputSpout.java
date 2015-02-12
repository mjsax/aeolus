package de.hub.cs.dbis.aeolus.queries.utils;

/*
 * #%L
 * utils
 * $Id:$
 * $HeadURL:$
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


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;





/**
 * {@link OrderedFileInputSpout} is an {@link OrderedInputSpout} that reads input tuples from multiple files. Each file
 * is an <em>input partition</em> in {@link OrderedInputSpout} terminology .The default file is {@code input}. Use
 * {@link #INPUT_FILE_NAME} and {@link #FILE_SUFFIXES} to specify different file name(s) in the topology configuration
 * (see {@link backtype.storm.Config}).
 * 
 * 
 * @author Leonardo Aniello (Sapienza Università di Roma, Roma, Italy)
 * @author Roberto Baldoni (Sapienza Università di Roma, Roma, Italy)
 * @author Leonardo Querzoni (Sapienza Università di Roma, Roma, Italy)
 * @author Matthias J. Sax
 */
public abstract class OrderedFileInputSpout extends OrderedInputSpout {
	private static final long serialVersionUID = -4690963122364704481L;
	
	private final Logger logger = LoggerFactory.getLogger(OrderedFileInputSpout.class);
	/**
	 * Can be used to specify an input file name (or prefix together with {@link #INPUT_FILE_SUFFIXES}). The
	 * configuration value is expected to be of type {@link String}.
	 */
	public static final String INPUT_FILE_NAME = "OrderedFileInputSpout.input";
	/**
	 * Can be used to specify a list of file name suffixes (one suffix for each input file) if multiple input files are
	 * used. {@link #INPUT_FILE_NAME} is used as file prefix for each file. The configuration value is expected to be of
	 * type {@link List}.
	 */
	public static final String INPUT_FILE_SUFFIXES = "OrderedFileInputSpout.inputFileSuffixes";
	/**
	 * The prefix of all input file names.
	 */
	private String prefix = "input";
	/**
	 * All input files to read from.
	 */
	private final ArrayList<BufferedReader> inputFiles = new ArrayList<BufferedReader>();
	
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		String fileName = (String)conf.get(INPUT_FILE_NAME);
		if(fileName != null) {
			this.prefix = fileName;
		}
		
		List<Object> suffixes = (List<Object>)conf.get(INPUT_FILE_SUFFIXES);
		if(suffixes != null) {
			// distribute input files over all tasks using round robin
			int componentTaskCount = context.getComponentTasks(context.getThisComponentId()).size();
			
			for(int index = context.getThisTaskIndex(); index < suffixes.size(); index += componentTaskCount) {
				try {
					this.inputFiles.add(new BufferedReader(new FileReader(this.prefix + suffixes.get(index))));
				} catch(FileNotFoundException e) {
					this.logger.error("Input file <{}> not found.", this.prefix + suffixes.get(index));
				}
			}
		} else {
			try {
				this.inputFiles.add(new BufferedReader(new FileReader(this.prefix)));
			} catch(FileNotFoundException e) {
				this.logger.error("Input file <{}> not found:", this.prefix);
			}
		}
		
		// need to create new HashMap because given one does not implement .put(...) method
		@SuppressWarnings("rawtypes")
		HashMap newConfig = new HashMap(conf);
		newConfig.put(NUMBER_OF_PARTITIONS, new Integer(this.inputFiles.size()));
		
		super.open(newConfig, context, collector);
	}
	
	@Override
	protected String readNextLine(int index) {
		try {
			return this.inputFiles.get(index).readLine();
		} catch(IOException e) {
			this.logger.error(e.toString());
		}
		
		return null;
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ts", "rawTuple"));
	}
	
	@Override
	public void close() {
		for(BufferedReader reader : this.inputFiles) {
			try {
				reader.close();
			} catch(IOException e) {
				this.logger.error("Closing input file reader failed.", e);
			}
		}
	}
	
	@Override
	public void activate() {/* empty */}
	
	@Override
	public void deactivate() {/* empty */}
	
	@Override
	public void ack(Object msgId) {
		// TODO
	}
	
	@Override
	public void fail(Object msgId) {
		// TODO
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
