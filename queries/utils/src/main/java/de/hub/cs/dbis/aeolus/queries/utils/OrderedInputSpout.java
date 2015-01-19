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
package de.hub.cs.dbis.aeolus.queries.utils;

import java.text.ParseException;
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
 * {@link OrderedInputSpout} reads input tuples (as {@link String}) from multiple sources (called <em>partitions</em>)
 * and pushes raw data into the topology.<br />
 * <br />
 * Each retrieved string from each partitions must contain a single tuple and input data must be sorted in ascending
 * timestamp order. For each successfully processed input string, a single output tuple is emitted.<br />
 * <br />
 * <strong>Output schema:</strong> {@code <ts:}{@link Long}{@code ,rawTuple:}{@link String}{@code >}<br />
 * Attribute {@code ts} contains the extracted timestamp value of the processed input line and {@code rawTuple} contains
 * the <em>complete</em> input line.<br />
 * <br />
 * {@link OrderedInputSpout} is parallelizable. If multiple input files are assigned to a single task,
 * {@link OrderedInputSpout} ensures that tuples are emitted in ascending timestamp order. In case of timestamp
 * duplicates, no ordering guarantee for all tuples having the same timestamp is given.
 * 
 * @author Leonardo Aniello (Sapienza Università di Roma, Roma, Italy)
 * @author Roberto Baldoni (Sapienza Università di Roma, Roma, Italy)
 * @author Leonardo Querzoni (Sapienza Università di Roma, Roma, Italy)
 * @author Matthias J. Sax
 */
public abstract class OrderedInputSpout implements IRichSpout {
	private static final long serialVersionUID = 6224448887936832190L;
	
	private final Logger logger = LoggerFactory.getLogger(OrderedInputSpout.class);
	/**
	 * Can be used to specify an number of input partitions that are available (default value is one). The configuration
	 * value is expected to be of type {@link Integer}.
	 */
	public static final String NUMBER_OF_PARTITIONS = "OrderedInputSpout.partitions";
	/**
	 * The number of available input partitions. Default value is one.
	 */
	protected int numberOfPartitons = 1;
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
	
	
	
	/**
	 * {@inheritDoc}
	 * 
	 * Initializing includes the access to each partition. Hence, {@link #readNextLine(int)} is called for each
	 * partition. Furthermore, {@link #parseTimestamp(String)} is called.
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") SpoutOutputCollector collector) {
		Integer numPartitions = (Integer)conf.get(NUMBER_OF_PARTITIONS);
		if(numPartitions != null) {
			this.numberOfPartitons = numPartitions.intValue();
		}
		
		this.collector = collector;
		
		this.lines = new String[this.numberOfPartitons];
		this.timestamp = new long[this.numberOfPartitons];
		// read first line of each input partition
		for(int i = 0; i < this.numberOfPartitons; i++) {
			this.processNextLine(i);
		}
	}
	
	@Override
	public void nextTuple() {
		// find tuple with smallest TS value from line buffer
		long minTs = Long.MAX_VALUE;
		int minTsIndex = -1;
		for(int i = 0; i < this.numberOfPartitons; i++) {
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
		
		this.processNextLine(minTsIndex);
	}
	
	private void processNextLine(int index) {
		while(true) {
			this.lines[index] = this.readNextLine(index);
			if(this.lines[index] == null) {
				this.logger.error("Could not read from partition {}.", new Integer(index));
				return; // break loop
			}
			
			try {
				if(this.lines[index] != null) {
					this.timestamp[index] = this.parseTimestamp(this.lines[index]);
				}
				
				return; // no error occurred; break loop
			} catch(ParseException e) {
				this.logger.error("Invalid tuple.", e);
				this.logger.warn("Dropping tuple <{}>", this.lines[index]);
			}
		}
	}
	
	/**
	 * Reads the next input line for the specified input partition.
	 * 
	 * @param index
	 *            The index of the input partition the next line must be read from.
	 */
	protected abstract String readNextLine(int index);
	
	/**
	 * Extracts the tuple's timestamp from the tuple's string representation.
	 * 
	 * @param line
	 *            The string representing a tuple.
	 * 
	 * @return The tuple's timestamp.
	 * 
	 * @throws ParseException
	 *             if the timestamp could not be extracted
	 */
	protected abstract long parseTimestamp(String line) throws ParseException;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ts", "rawTuple"));
	}
	
	@Override
	public void close() {/* empty */}
	
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
