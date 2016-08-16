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

import java.util.Arrays;
import java.util.HashMap;
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
import de.hub.cs.dbis.aeolus.utils.StreamMerger;





/**
 * {@link AbstractOrderedInputSpout} reads input tuples (of type {@code T}) from multiple sources (called
 * <em>partitions</em>) and pushes raw data into the topology. The default number of used partitions is one but can be
 * configured using {@link #NUMBER_OF_PARTITIONS}. The IDs of the partitions are {@code 0,...,NUMBER_OF_PARTITIONS-1}.<br/>
 * <br/>
 * Input data must be sorted in ascending timestamp order in each partition. For each successfully processed input
 * tuple, a single output tuple is emitted.<br/>
 * <br/>
 * <strong>Output schema:</strong> {@code <ts:}{@link Long}{@code ,rawTuple:T>}<br/>
 * Attribute {@code ts} contains the extracted timestamp value of the processed input tuple and {@code rawTuple}
 * contains the <em>complete</em> input tuple.<br/>
 * <br/>
 * {@link AbstractOrderedInputSpout} is parallelizable. If multiple input partitions are assigned to a single task,
 * {@link AbstractOrderedInputSpout} ensures that tuples are emitted in ascending timestamp order. In case of timestamp
 * duplicates, no ordering guarantee for all tuples having the same timestamp is given.
 * 
 * @author Leonardo Aniello (Sapienza Università di Roma, Roma, Italy)
 * @author Roberto Baldoni (Sapienza Università di Roma, Roma, Italy)
 * @author Leonardo Querzoni (Sapienza Università di Roma, Roma, Italy)
 * @author mjsax
 */
public abstract class AbstractOrderedInputSpout<T> implements IRichSpout {
	private final static long serialVersionUID = 6224448887936832190L;
	private final static Logger logger = LoggerFactory.getLogger(AbstractOrderedInputSpout.class);
	
	/**
	 * Can be used to specify the number of input partitions that are available (default value is one). The
	 * configuration value is expected to be of type {@link Integer}.
	 */
	public final static String NUMBER_OF_PARTITIONS = "OrderedInputSpout.partitions";
	
	/** The merger to be used. */
	private StreamMerger<Values> merger;
	
	/** The output collector to be used. */
	private SpoutOutputCollector collector;
	
	/**
	 * The stream ID to be used for declaration of timestamp and raw tuple fields. Can be {@code null} which causes the
	 * fields to be declared only.
	 */
	private final String streamID;
	
	
	
	/**
	 * Creates a {@code AbstractOrderedInputSpout} which declares fields without explicit stream ID.
	 */
	public AbstractOrderedInputSpout() {
		this(null);
	}
	
	/**
	 * Creates a {@code AbstractOrderedInputSpout} which declares fields on stream with ID {@code streamID}.
	 * 
	 * @param streamID
	 *            the ID of the stream the fields ought to be declared on
	 */
	public AbstractOrderedInputSpout(String streamID) {
		this.streamID = streamID;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * Sets up internal data structures according to the number of used partitions {@link #NUMBER_OF_PARTITIONS}.
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		int numberOfPartitons = 1;
		Integer numPartitions = (Integer)conf.get(NUMBER_OF_PARTITIONS);
		if(numPartitions != null) {
			numberOfPartitons = numPartitions.intValue();
		}
		logger.debug("Number of configured partitions: {}", new Integer(numberOfPartitons));
		
		Integer[] partitionIds = new Integer[numberOfPartitons];
		for(int i = 0; i < numberOfPartitons; ++i) {
			partitionIds[i] = new Integer(i);
		}
		this.merger = new StreamMerger<Values>(Arrays.asList(partitionIds), 0);
		this.collector = collector;
	}
	
	/**
	 * Makes a new output tuple available.
	 * 
	 * Must be used by {@link #nextTuple()} instead of an {@link SpoutOutputCollector} to emit tuples. In each call, all
	 * tuples that are still buffered over all partitions are considered to be emitted to the default output stream.
	 * 
	 * @param index
	 *            The partition id the tuple belongs to.
	 * @param timestamp
	 *            The timestamp of the tuple.
	 * @param tuple
	 *            The tuple to be emitted.
	 * 
	 * @return A map of all tuple that got emitted during this call including the task IDs each emitted tuple was sent
	 *         to.
	 */
	// TODO: add support for non-default and/or multiple output streams (what about directEmit(...)?)
	protected final Map<Values, List<Integer>> emitNextTuple(Integer index, Long timestamp, T tuple) {
		logger.trace("Received new output tuple (partitionId, ts, tuple): {}, {}, {}", index, timestamp, tuple);
		if(index != null && timestamp != null && tuple != null) {
			this.merger.addTuple(index, new Values(timestamp, tuple));
		}
		
		Values t;
		Map<Values, List<Integer>> emitted = new HashMap<Values, List<Integer>>();
		while((t = this.merger.getNextTuple()) != null) {
			logger.trace("Emitting tuple: {}", t);
			emitted.put(t, this.collector.emit(t));
		}
		
		return emitted;
	}
	
	/**
	 * Closes an input partition. Closing a partition is only successful, if no tuples belonging to the partition are
	 * buffered internally any more. No more data can be emitted by this partition if closing was successful.
	 * 
	 * @param partitionId
	 *            The ID of the partition to be closed.
	 * 
	 * @return {@code true} if the partition was closed successfully -- {@code false} otherwise
	 */
	protected boolean closePartition(Integer partitionId) {
		return this.merger.closePartition(partitionId);
	}
	
	/**
	 * Declares the two fields necessary for transmitting tuples with a timestamp. Calling
	 * {@code super.declareOutputFields} in overriding methods is strongly recommended.
	 * 
	 * @param declarer
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields("ts", "rawTuple");
		if(this.streamID == null) {
			declarer.declare(fields);
		} else {
			declarer.declareStream(this.streamID, fields);
		}
	}
	
}
