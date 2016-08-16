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
package de.hub.cs.dbis.aeolus.monitoring.throughput;

import java.util.HashMap;
import java.util.Map.Entry;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;





/**
 * {@link AbstractThroughputCounter} counts the number of incoming or outgoing tuples of a single Spout or Bolt task. It
 * computes a separate count for each logical stream and the overall count over all streams. The collected statistical
 * values can be emitted to a regular stream within a topology.<br />
 * <br />
 * <strong>Statistics report stream output schema:</strong> {@code <ts:}{@link Long}{@code ,streamId:}{@link String}
 * {@code ,count:}{@link Long} {@code ,delta:}{@link Long}{@code >}<br />
 * <br />
 * where {@code ts} is the timestamp when reporting is triggered, {@code streamId} is the stream name the statistics
 * belong to, {@code count} is the overall count, and {@code delta} is the count since the last report. <em>Be aware,
 * that each declared input or output stream is prefixed with</em> {@code in::} <em>or</em> {@code out::}
 * <em>respectively. Additionally, the stream IDs</em> {@code in} <em>and</em> {@code out}
 * <em>are used for the counters over all incoming or outgoing streams.</em>
 * 
 * @author mjsax
 */
abstract class AbstractThroughputCounter {
	
	/** The name of the timestamp attribute. */
	public final static String TS_ATTRIBUTE = "ts";
	
	/** The name of the stream ID attribute. */
	public final static String STREAM_ID_ATTRIBUTE = "streamId";
	
	/** The name of the task ID attribute. */
	public final static String TASK_ID_ATTRIBUTE = "taskId";
	
	/** The name of the overall count attribute. */
	public final static String COUNT_ATTRIBUTE = "count";
	
	/** The name of the delta count attribute. */
	public final static String DELTA_ATTRIBUTE = "delta";
	
	/** The name of the throughput attribute. */
	public final static String THROUGHPUT_ATTRIBUTE = "throughput";
	
	/** The index of the timestamp attribute. */
	public final static int TS_INDEX = 0;
	
	/** The index of the stream ID attribute. */
	public final static int STREAM_ID_INDEX = 1;
	
	/** The index of the overall count attribute. */
	public final static int COUNT_INDEX = 2;
	
	/** The index of the delta count attribute. */
	public final static int DELTA_INDEX = 3;
	
	
	
	/**
	 * Indicates if this counter is used to count input or output streams. The value is {@code true} or {@code false}
	 * for input or output streams, respectively.
	 */
	private final boolean inputOrOutput;
	
	/** The ID of the task reporting the metric. */
	private final Integer taskId;
	
	/** Holds the overall count per stream. */
	private final HashMap<String, Counter> counter = new HashMap<String, Counter>();
	
	/** Hold the count per stream since the last report. */
	private final HashMap<String, Counter> deltaCounter = new HashMap<String, Counter>();
	
	/** The overall count over all streams. */
	private long overallCount = 0;
	
	
	
	/**
	 * Instantiates a new throughput-counter.
	 * 
	 * @param inputOrOutput
	 *            Indicates if input ({@code true}) or output ({@code false}) streams are monitored.
	 * @param taskId
	 *            The task ID.
	 */
	public AbstractThroughputCounter(boolean inputOrOutput, int taskId) {
		this.inputOrOutput = inputOrOutput;
		this.taskId = taskId;
	}
	
	
	
	/**
	 * Increases the counter of an input stream.
	 * 
	 * @param streamId
	 *            The ID of the input stream.
	 */
	void countIn(String streamId) {
		assert (this.inputOrOutput);
		this.doCount("in::" + streamId);
	}
	
	/**
	 * Increases the count of an output stream.
	 * 
	 * @param streamId
	 *            The ID of the output stream.
	 */
	void countOut(String streamId) {
		assert (!this.inputOrOutput);
		this.doCount("out::" + streamId);
	}
	
	/**
	 * Performs the actual counting for input and output streams.
	 * 
	 * @param streamId
	 *            The ID of the stream.
	 */
	private synchronized void doCount(String streamId) {
		Counter c = this.deltaCounter.get(streamId);
		if(c == null) {
			c = new Counter();
			this.deltaCounter.put(streamId, c);
		}
		++c.counter;
	}
	
	/**
	 * Reports all count values of each stream and the overall count over all streams.
	 * 
	 * @param ts
	 *            The timestamp when this report is triggered.
	 * @param factor
	 *            The normalization factor for reported values "per second".
	 */
	synchronized void reportCount(long ts, double factor) {
		long overallDelta = 0;
		
		for(Entry<String, Counter> count : this.deltaCounter.entrySet()) {
			String streamId = count.getKey();
			Counter delta = count.getValue();
			
			Counter c = this.counter.get(streamId);
			if(c == null) {
				c = new Counter();
				this.counter.put(streamId, c);
			}
			c.counter += delta.counter;
			overallDelta += delta.counter;
			this.doEmit(new Values(new Long(ts), streamId, taskId, new Long(c.counter), new Long(delta.counter),
				new Long((long)(delta.counter / factor))));
			
			count.setValue(new Counter());
		}
		
		this.overallCount += overallDelta;
		String id;
		if(this.inputOrOutput) {
			id = "in";
		} else {
			id = "out";
		}
		
		this.doEmit(new Values(new Long(ts), id, taskId, new Long(this.overallCount), new Long(overallDelta), new Long(
			(long)(overallDelta / factor))));
	}
	
	/**
	 * Reports a single statistical value.
	 * 
	 * @param statsTuple
	 *            The report tuple to be emitted.
	 */
	abstract void doEmit(Values statsTuple);
	
	/**
	 * Declares an report stream with schema {@code <ts:}{@link Long}{@code ,streamId:}{@link String} {@code ,count:}
	 * {@link Long} {@code ,delta:}{@link Long}{@code >}
	 * 
	 * @param reportStream
	 *            The ID of the report stream to be declared.
	 * @param declarer
	 *            The declarer object the report stream is declared to.
	 */
	static void declareStatsStream(String reportStream, OutputFieldsDeclarer declarer) {
		declarer.declareStream(reportStream, new Fields(TS_ATTRIBUTE, STREAM_ID_ATTRIBUTE, TASK_ID_ATTRIBUTE,
			COUNT_ATTRIBUTE, DELTA_ATTRIBUTE, THROUGHPUT_ATTRIBUTE));
	}
	
}
