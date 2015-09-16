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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;





/**
 * {@link AbstractFileOutputBolt} writes all received tuples to an output file. The output file name can be configured
 * using {@link #outputDirName} and {@link #outputFileName} (default is {@code ./result.dat}).<br/>
 * <br/>
 * {@link AbstractFileOutputBolt} acknowledges each retrieved tuple.
 * 
 * @author Matthias J. Sax
 */
public abstract class AbstractFileOutputBolt implements IRichBolt {
	private final static long serialVersionUID = 5082995927274164044L;
	private final static Logger logger = LoggerFactory.getLogger(AbstractFileOutputBolt.class);
	
	/** Can be used to specify an output file name. The configuration value is expected to be of type {@link String}. */
	public final static String OUTPUT_FILE_NAME = "FileOutputBolt.outputFile";
	
	/** Can be used to specify an output directory. The configuration value is expected to be of type {@link String}. */
	public final static String OUTPUT_DIR_NAME = "FileOutputBolt.outputDir";
	
	/** The name of the output file. */
	private String outputFileName = "result.dat";
	
	/** The name of the output directory. */
	private String outputDirName = ".";
	
	/** The output collector to be used. */
	private OutputCollector collector;
	
	/** The used file writer. */
	private BufferedWriter writer = null;
	
	
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
		String fileName = (String)stormConf.get(OUTPUT_FILE_NAME);
		if(fileName != null) {
			this.outputFileName = fileName;
		}
		
		String dirName = (String)stormConf.get(OUTPUT_DIR_NAME);
		if(dirName != null) {
			this.outputDirName = dirName;
		}
		
		try {
			this.writer = new BufferedWriter(new FileWriter(this.outputDirName + File.separator + this.outputFileName));
		} catch(IOException e) {
			logger.error("Could not open output file <{}> for writing.", this.outputDirName + File.separator
				+ this.outputFileName);
		}
		
		this.collector = collector;
	}
	
	/**
	 * Writes the given tuple to disc (see {@link #tupleToString(Tuple)}.
	 * 
	 * @param input
	 *            The input tuple to be processed.
	 */
	@Override
	public void execute(Tuple input) {
		if(this.writer != null) {
			try {
				this.writer.write(this.tupleToString(input));
			} catch(IOException e) {
				logger.error("Could not output tuple to output file: {}", input);
			}
		}
		this.collector.ack(input);
	}
	
	/**
	 * Same as {@link #execute(Tuple)}, but flushed the written output data to disc immediately.
	 * 
	 * @param input
	 *            The input tuple to be processed.
	 */
	public void executeAndFlush(Tuple input) {
		if(this.writer != null) {
			try {
				this.writer.write(this.tupleToString(input));
				this.writer.flush();
			} catch(IOException e) {
				logger.error("Could not output tuple to output file: {}", input);
			}
		}
		this.collector.ack(input);
	}
	
	/**
	 * Transforms a {@link Tuple} into a {@link String}. The returned string is written to the output file.
	 * 
	 * @param t
	 *            The output tuple.
	 * 
	 * @return The string representation of the tuple.
	 */
	protected abstract String tupleToString(Tuple t);
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {/* nothing to do; no output stream is generated */}
	
	@Override
	public void cleanup() {
		if(this.writer != null) {
			try {
				this.writer.close();
			} catch(IOException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
