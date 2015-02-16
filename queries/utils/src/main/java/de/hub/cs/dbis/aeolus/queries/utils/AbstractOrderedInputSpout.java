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


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;





/**
 * {@link AbstractOrderedInputSpout} reads input tuples (of type {@code T}) from multiple sources (called
 * <em>partitions</em>) and pushes raw data into the topology. The default number of used partitions is one but can be
 * configured using {@link #NUMBER_OF_PARTITIONS}. The IDs of the partitions are {@code 0,...,NUMBER_OF_PARTITIONS-1}.<br />
 * <br />
 * Input data must be sorted in ascending timestamp order from each partition. For each successfully processed input
 * tuple, a single output tuple is emitted.<br />
 * <br />
 * <strong>Output schema:</strong> {@code <ts:}{@link Long}{@code ,rawTuple:T>}<br />
 * Attribute {@code ts} contains the extracted timestamp value of the processed input line and {@code rawTuple} contains
 * the <em>complete</em> input tuple.<br />
 * <br />
 * {@link AbstractOrderedInputSpout} is parallelizable. If multiple input partitions are assigned to a single task,
 * {@link AbstractOrderedInputSpout} ensures that tuples are emitted in ascending timestamp order. In case of timestamp
 * duplicates, no ordering guarantee for all tuples having the same timestamp is given.
 * 
 * @author Leonardo Aniello (Sapienza Università di Roma, Roma, Italy)
 * @author Roberto Baldoni (Sapienza Università di Roma, Roma, Italy)
 * @author Leonardo Querzoni (Sapienza Università di Roma, Roma, Italy)
 * @author Matthias J. Sax
 */
public abstract class AbstractOrderedInputSpout<T> implements IRichSpout {
	private static final long serialVersionUID = 6224448887936832190L;
	
	
	
	/**
	 * Can be used to specify an number of input partitions that are available (default value is one). The configuration
	 * value is expected to be of type {@link Integer}.
	 */
	public static final String NUMBER_OF_PARTITIONS = "OrderedInputSpout.partitions";
	/**
	 * The merger to be used.
	 */
	private StreamMerger<Values> merger;
	/**
	 * The output collector to be used.
	 */
	private SpoutOutputCollector collector;
	
	
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	public final void open(Map conf, TopologyContext context, @SuppressWarnings("hiding") SpoutOutputCollector collector) {
		// need to create new map because given one is read only
		this.openSimple(new HashMap(conf), context);
		
		int numberOfPartitons = 1;
		Integer numPartitions = (Integer)conf.get(NUMBER_OF_PARTITIONS);
		if(numPartitions != null) {
			numberOfPartitons = numPartitions.intValue();
		}
		
		Integer[] partitionIds = new Integer[numberOfPartitons];
		for(int i = 0; i < numberOfPartitons; ++i) {
			partitionIds[i] = new Integer(i);
		}
		this.merger = new StreamMerger<Values>(Arrays.asList(partitionIds), 0);
		this.collector = collector;
	}
	
	/**
	 * Replaces the regular {@link #open(Map, TopologyContext, SpoutOutputCollector)} method and will be called by the
	 * regular one at the very beginning.
	 * 
	 * @param conf
	 *            The Storm configuration for this spout. This is the configuration provided to the topology merged in
	 *            with cluster configuration on this machine.
	 * @param context
	 *            This object can be used to get information about this task's place within the topology, including the
	 *            task id and component id of this task, input and output information, etc.
	 */
	protected void openSimple(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		// empty
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
		assert (index != null);
		assert (timestamp != null);
		assert (tuple != null);
		
		this.merger.addTuple(index, new Values(timestamp, tuple));
		
		Values t;
		Map<Values, List<Integer>> emitted = new HashMap<Values, List<Integer>>();
		while((t = this.merger.getNextTuple()) != null) {
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
	
	@Override
	public final void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ts", "rawTuple"));
	}
	
}
