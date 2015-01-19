/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package debs2013;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;





/**
 * {@link SensorSpout} reads sensor values from multiple files and pushes raw sensor data into the topology. The default
 * file is {@code sensor}. Use {@link #INPUT_FILE_NAME} and {@link #FILE_SUFFIXES} to specify different file name(s) in
 * the topology configuration (see {@link backtype.storm.Config}).<br />
 * <br />
 * The file must be in CSV-format, each line containing a single tuple ({@code <sid,ts,x,y,z,|v|,|a|,vx,vy,vz,ax,ay,az>}
 * ). Input data must be sorted in ascending timestamp order. For each successfully processed input line, a single
 * output tuple is emitted.<br />
 * <br />
 * <strong>Output schema:</strong> {@code <ts:}{@link Long}{@code ,rawTuple:}{@link String}{@code >}<br />
 * Attribute {@code ts} contains the extracted timestamp value of the processed input line and {@code rawTuple} contains
 * the <em>complete</em> input line.<br />
 * <br />
 * {@link SensorSpout} is parallelizable. If multiple input files are assigned to a single task, {@link SensorSpout}
 * ensures that tuples are emitted in ascending timestamp order. In case of timestamp duplicates, no ordering guarantee
 * for all tuples having the same timestamp is given.
 * 
 * @author Leonardo Aniello (Sapienza Università di Roma, Roma, Italy)
 * @author Roberto Baldoni (Sapienza Università di Roma, Roma, Italy)
 * @author Leonardo Querzoni (Sapienza Università di Roma, Roma, Italy)
 * @author Matthias J. Sax
 */
public class SensorSpout implements IRichSpout {
	private static final long serialVersionUID = -4690963122364704481L;
	
	private final Logger logger = LoggerFactory.getLogger(SensorSpout.class);
	/**
	 * Can be used to specify an input file name (or prefix together with {@link #INPUT_FILE_SUFFIXES}). The
	 * configuration value is expected to be of type {@link String}.
	 */
	public static final String INPUT_FILE_NAME = "debs2013.q1.input";
	/**
	 * Can be used to specify a list of file name suffixes (one suffix for each input file) if multiple input files are
	 * used. {@link #INPUT_FILE_NAME} is used as file prefix for each file. The configuration value is expected to be of
	 * type {@link List}.
	 */
	public static final String INPUT_FILE_SUFFIXES = "debs2013.q1.inputFileSuffixes";
	/**
	 * The prefix of all input file names.
	 */
	private String prefix = "sensor";
	/**
	 * All input files to read from.
	 */
	private ArrayList<BufferedReader> inputFiles = new ArrayList<BufferedReader>();
	/**
	 * Buffer that holds the latest read line per input file.
	 */
	private String lines[];
	/**
	 * Buffer that holds the timestamp of the latest read line per input file.
	 */
	private long timestamp[];
	/**
	 * The output collector to be used.
	 */
	private SpoutOutputCollector collector;
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") SpoutOutputCollector collector) {
		String fileName = (String)conf.get(INPUT_FILE_NAME);
		if(fileName != null) {
			this.prefix = fileName;
		}
		
		@SuppressWarnings("unchecked")
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
		
		this.collector = collector;
		
		this.lines = new String[this.inputFiles.size()];
		this.timestamp = new long[this.lines.length];
		// read first line of each input file
		for(int i = 0; i < this.lines.length; i++) {
			this.readNextLine(i);
		}
	}
	
	@Override
	public void nextTuple() {
		// find tuple with smallest TS value from line buffer
		long minTs = Long.MAX_VALUE;
		int minTsIndex = -1;
		for(int i = 0; i < this.lines.length; i++) {
			if(this.lines[i] != null) {
				if(minTs > this.timestamp[i]) {
					minTs = this.timestamp[i];
					minTsIndex = i;
				}
			}
		}
		
		if(minTsIndex == -1) {
			this.logger.warn("All input streams finished.");
			return;
		}
		
		// TODO add messageID for fault-tolerance
		this.collector.emit(new Values(new Long(minTs), this.lines[minTsIndex]));
		
		this.readNextLine(minTsIndex);
	}
	
	private void readNextLine(int index) {
		while(true) {
			try {
				this.lines[index] = this.inputFiles.get(index).readLine();
			} catch(IOException e) {
				this.logger.error("Could not read from input file.", e);
				this.lines[index] = null; // set this input file to "finished"; we will not try to read from it again
				
				return; // break loop
			}
			
			try {
				if(this.lines[index] != null) {
					int p1 = this.lines[index].indexOf(",");
					int p2 = this.lines[index].indexOf(",", p1 + 1);
					this.timestamp[index] = Long.parseLong(this.lines[index].substring(p1 + 1, p2));
				}
				
				return; // no error occurred; break loop
			} catch(IndexOutOfBoundsException e) {
				this.logger.error("Invalid tuple.", e);
				this.logger.warn("Tupel should be in CSV format containing 13 entries. Dropping tuple <{}>",
					this.lines[index]);
			} catch(NumberFormatException e) {
				this.logger.error("Invalid tuple.", e);
				this.logger.warn("Tupel should be in CSV format containing 13 entries. Dropping tuple <{}>",
					this.lines[index]);
			}
		}
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
