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
package storm.lrb.model;

import java.io.Serializable;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.lrb.bolt.SegmentIdentifier;
import storm.lrb.tools.StopWatch;
import backtype.storm.tuple.Values;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;





/**
 * superclass for all requests
 * 
 */
/*
 * internal implementation notes: - implents Values in order to allow field grouping in strom topologies
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public abstract class LRBtuple extends Values implements Serializable {
	
	// official LRB types
	public final static int TYPE_POSITION_REPORT = 0;
	public final static int TYPE_ACCOUNT_BALANCE_REQUEST = 2;
	public final static int TYPE_DAILY_EXPEDITURE = 3;
	public final static int TYPE_TRAVEL_TIME_REQUEST = 4;
	// additional (non-LRB) types
	public final static int TYPE_ACCOUNT_BALANCE = 5;
	
	private final static long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(LRBtuple.class);
	
	private static final Set<Integer> ALLOWED_TYPES = Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(
		TYPE_POSITION_REPORT, TYPE_ACCOUNT_BALANCE_REQUEST, TYPE_DAILY_EXPEDITURE, TYPE_TRAVEL_TIME_REQUEST,
		TYPE_ACCOUNT_BALANCE)));
	
	/**
	 * tuple type 0=Position report 2=Account balance requests 3=daily expenditure request 4=Travel time request
	 */
	private Integer type;
	
	/**
	 * time of creation (in the storm application)
	 */
	private Long created;
	
	private StopWatch timer;
	
	/**
     *
     */
	private Integer sinit;
	/**
     *
     */
	private Integer send;
	/**
	 * DOW (1. . .7) specify the day of the week
	 */
	private Integer dow;
	/**
	 * TOD (1. . .1440) specifies the day of the week and minute number in the day when the journey would take place
	 */
	private Integer tod;
	
	public LRBtuple() {
		// kryo needs empty constructor
	}
	
	public LRBtuple(Integer type, Long created, StopWatch systemTimer) {
		if(!ALLOWED_TYPES.contains(type)) {
			throw new IllegalArgumentException(String.format("type '%s' is not allowed (allowed types are '%s'", type,
				ALLOWED_TYPES));
		}
		this.type = type;
		this.created = created;
		this.timer = systemTimer;
		this.timer.setOffset(this.created);
	}
	
	public Integer getType() {
		return this.type;
	}
	
	public Integer getSinit() {
		return this.sinit;
	}
	
	public Integer getSend() {
		return this.send;
	}
	
	public Integer getDow() {
		return this.dow;
	}
	
	public Integer getTod() {
		return this.tod;
	}
	
	public StopWatch getTimer() {
		return this.timer;
	}
	
	/**
	 * Time of creation. (actual running time of the simulation)
	 * 
	 * @return
	 */
	public Long getCreated() {
		return this.created;
	}
	
	/**
	 * get the time it took to process this tuple in ms
	 * 
	 * @return processing time in ms
	 */
	public long getProcessingTime() {
		return this.timer.getDurationTime();
	}
	
	/**
	 * get the time it took to process this tuple in ms
	 * 
	 * @return processing time in seconds.
	 */
	public long getProcessingTimeSec() {
		return this.timer.getDurationTimeSecs();
	}
	
}
