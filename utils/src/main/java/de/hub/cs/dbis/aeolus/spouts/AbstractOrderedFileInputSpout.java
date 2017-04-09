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
package de.hub.cs.dbis.aeolus.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;





/**
 * {@link AbstractOrderedFileInputSpout} is an {@link AbstractOrderedInputSpout} that reads input tuples line by line
 * from multiple files (ie, each line must contain exactly one tuple). Each file is an <em>input partition</em> in
 * {@link AbstractOrderedInputSpout} terminology. The default file is {@code input}. Use {@link #INPUT_FILE_NAME} and
 * {@link #INPUT_FILE_SUFFIXES} to specify different file name(s) in the topology configuration (see
 * {@link backtype.storm.Config}).<br/>
 * <br/>
 * <strong>Output schema:</strong> {@code <ts:}{@link Long}{@code ,rawTuple:}{@link String}{@code >}<br/>
 * 
 * @author Leonardo Aniello (Sapienza Università di Roma, Roma, Italy)
 * @author Roberto Baldoni (Sapienza Università di Roma, Roma, Italy)
 * @author Leonardo Querzoni (Sapienza Università di Roma, Roma, Italy)
 * @author mjsax
 */
public abstract class AbstractOrderedFileInputSpout extends AbstractOrderedInputSpout<String> {
	private final static long serialVersionUID = -4690963122364704481L;
	private final static Logger logger = LoggerFactory.getLogger(AbstractOrderedFileInputSpout.class);
	
	/**
	 * Can be used to specify an input file name (or prefix together with {@link #INPUT_FILE_SUFFIXES}). The
	 * configuration value is expected to be of type {@link String}.
	 */
	public final static String INPUT_FILE_NAME = "OrderedFileInputSpout.input";
	
	/**
	 * Can be used to specify a list of file name suffixes (one suffix for each input file) if multiple input files are
	 * used. {@link #INPUT_FILE_NAME} is used as file prefix for each file. The configuration value is expected to be of
	 * type {@link List}.
	 */
	public final static String INPUT_FILE_SUFFIXES = "OrderedFileInputSpout.inputFileSuffixes";
	
	/** The prefix of all input file names. */
	private String prefix = "input";
	
	/** All input files to read from. */
	private final ArrayList<BufferedReader> inputFiles = new ArrayList<BufferedReader>();
	
	/** Emit-Round-Robin-Index. */
	private int emitIndex = -1;
	
	/**
	 * Map containing all tuples emitted by the last call of {@link #emitNextTuple(Integer, Long, Object)}. The map
	 * including the task IDs each tuple was sent to.
	 */
	protected Map<Values, List<Integer>> emitted = new HashMap<Values, List<Integer>>();
	
	
	
	/**
	 * Creates a {@code AbstractOrderedFileInputSpout} which declares fields without explicit stream ID.
	 */
	public AbstractOrderedFileInputSpout() {}
	
	/**
	 * Creates a {@code AbstractOrderedFileInputSpout} which declares fields on stream with ID {@code streamID}.
	 * 
	 * @param streamID
	 *            the ID of the stream the fields ought to be declared on
	 */
	public AbstractOrderedFileInputSpout(String streamID) {
		super(streamID);
	}
	
	
	
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
					logger.debug("Adding partition input file {}", this.prefix + suffixes.get(index));
					this.inputFiles.add(new BufferedReader(new FileReader(this.prefix + suffixes.get(index))));
				} catch(FileNotFoundException e) {
					logger.error("Input file <{}> not found.", this.prefix + suffixes.get(index));
				}
			}
		} else {
			try {
				logger.debug("Adding single input file {}", this.prefix);
				this.inputFiles.add(new BufferedReader(new FileReader(this.prefix)));
			} catch(FileNotFoundException e) {
				logger.error("Input file <{}> not found:", this.prefix);
			}
		}
		
		@SuppressWarnings("rawtypes")
		Map newConfig = new HashMap(conf); // need to copy into new HashMap because given one is read-only
		newConfig.put(NUMBER_OF_PARTITIONS, new Integer(this.inputFiles.size()));
		super.open(newConfig, context, collector);
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Reads a line from at least one input file, and emits all eligible tuples (including previously buffered once).
	 * The emitted tuples can be retrieved via {@link #emitted}.
	 */
	@Override
	public void nextTuple() {
		final int numberOfFiles = this.inputFiles.size();
		for(int i = 0; i < numberOfFiles; ++i) {
			this.emitIndex = (this.emitIndex + 1) % numberOfFiles;
			
			if(this.inputFiles.get(this.emitIndex) == null) { // check for closed partition
				continue;
			}
			String line = null;
			Integer emitIndexAsObject = new Integer(this.emitIndex);
			try {
				logger.trace("Read from partition {}", emitIndexAsObject);
				line = this.inputFiles.get(this.emitIndex).readLine();
			} catch(IOException e) {
				logger.error(e.toString());
			}
			if(line != null) {
				try {
					this.emitted = super.emitNextTuple(emitIndexAsObject, new Long(this.extractTimestamp(line)), line);
					
					logger.trace("Emitted the following tuples {}", this.emitted);
					if(this.emitted.size() != 0) {
						return;
					}
				} catch(ParseException e) {
					logger.error(e.toString());
				}
			} else {
				logger.debug("Try to close empty partition {}", emitIndexAsObject);
				if(super.closePartition(emitIndexAsObject)) {
					try {
						this.inputFiles.get(this.emitIndex).close();
					} catch(IOException e) {
						logger.error("Closing input file reader failed.", e);
					}
					// set to null but do not remove from list -> would change partition IDs in super.emitNextTuple
					this.inputFiles.set(this.emitIndex, null);
				} else {
					// we cannot put any more data,
					this.emitted = super.emitNextTuple(null, null, null);
					logger.trace("Emitted the following tuples {}", this.emitted);
				}
			}
		}
	}
	
	/**
	 * Extracts the timestamp from the given tuple.
	 * 
	 * @param tuple
	 *            The tuple to be processed.
	 * 
	 * @return The tuple's timestamp.
	 * 
	 * @throws ParseException
	 *             if the timestamp could not be extracted
	 */
	protected abstract long extractTimestamp(String tuple) throws ParseException;
	
	@Override
	public void close() {
		for(BufferedReader reader : this.inputFiles) {
			try {
				if(reader != null) {
					reader.close();
				}
			} catch(IOException e) {
				logger.error("Closing input file reader failed.", e);
			}
		}
	}
	
}
