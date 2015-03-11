package storm.lrb.model;

/*
 * #%L
 * lrb
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
public class LRBtuple extends Values implements Serializable {
	
	public final static int TYPE_POSITION_REPORT = 0;
	public final static int TYPE_ACCOUNT_BALANCE = 2;
	public final static int TYPE_DAILY_EXPEDITURE = 3;
	public final static int TYPE_TRAVEL_TIME_REQUEST = 4;
	
	private final static long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(LRBtuple.class);
	
	private static StopWatch retrieveTimeFromTuple(String tuple) {
		String[] tupleSplit = tuple.split(",");
		if(tupleSplit.length < 1) {
			LOGGER.debug(String.format("tuple line '%s' doesn't contain a valid time value", tuple));
		}
		String timeString = tupleSplit[0];
		long time0 = Long.parseLong(timeString);
		return new StopWatch(time0);
	}
	
	private static final Set<Integer> ALLOWED_TYPES = Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(PosReport.TYPE, AccBalRequest.TYPE, DaiExpRequest.TYPE, TravelTimeRequest.TYPE)));
	
	/**
	 * tuple type 0=Position report 2=Account balance requests 3=daily expenditure request 4=Travel time request
	 */
	private Integer type;
	
	/**
	 * time of creation (in the storm application)
	 */
	private Long created;
	
	/**
	 * Time (0. . .10799)^3 is a timestamp identifying the time at which the position report was emitted
	 */
	private int time;
	
	/**
	 * VID (0. . . MAXINT) is an integer vehicle identifier i
	 */
	private Integer vehicleIdentifier;
	
	/**
	 * Spd (0. . .100) is an integer reﬂecting the speed of the vehicle (in MPH) at the time the position report
	 */
	private int currentSpeed;
	/**
	 * Lane (0. . .4) identiﬁes the lane of the expressway from which the position report is emitted 0 if it is an
	 * entrance ramp (ENTRY), 1 − 3 if it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT).
	 */
	private Integer lane;
	private SegmentIdentifier segmentIdentifier;
	/**
	 * Pos (0. . .527999) identiﬁes the horizontal position of the vehicle as a measure of the number of feet from the
	 * western most point on the expressway (i.e.,Pos = x)
	 */
	private Integer position;
	/**
	 * QID is an integer query identiﬁer
	 */
	private Integer queryIdentifier;
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
	/**
	 * day (1 is yesterday, 69 is 10 weeks ago)
	 */
	private Integer day;
	
	private StopWatch timer;
	
	private StopWatch stormTimer = null;
	
	public LRBtuple() {
		// kryo needs empty constructor
	}

	public LRBtuple(Integer type, Long created, int time, Integer vehicleIdentifier, int currentSpeed, Integer lane, SegmentIdentifier segmentIdentifier, Integer position, Integer queryIdentifier, Integer sinit, Integer send, Integer dow, Integer tod, Integer day, StopWatch timer) {
		if(!ALLOWED_TYPES.contains(type)) {
			throw new IllegalArgumentException(String.format("type '%s' is not allowed (allowed types are '%s'", type, ALLOWED_TYPES));
		}
		this.type = type;
		this.created = created;
		this.time = time;
		this.vehicleIdentifier = vehicleIdentifier;
		this.currentSpeed = currentSpeed;
		this.lane = lane;
		this.segmentIdentifier = segmentIdentifier;
		this.position = position;
		this.queryIdentifier = queryIdentifier;
		this.sinit = sinit;
		this.send = send;
		this.dow = dow;
		this.tod = tod;
		this.day = day;
		this.timer = timer;
	}	
	
	public LRBtuple(String tuple, StopWatch systemtimer) {
		this.timer = new StopWatch(0);
		
		String[] result = tuple.split(",");
		LOGGER.debug("splitted tuple is '%s'", Arrays.toString(result));
		if(result.length < 15) {
			throw new IllegalArgumentException("Tuple does not match required format");
		}
		
		this.type = Integer.valueOf(result[0]);
		this.time = Integer.valueOf(result[1]);
		this.timer.setOffset(this.time);
		
		this.vehicleIdentifier = Integer.valueOf(result[2]);
		this.currentSpeed = Integer.valueOf(result[3]);
		int xway = Integer.valueOf(result[4]);
		this.lane = Integer.valueOf(result[5]);
		int direction = Integer.valueOf(result[6]);
		int segment = Integer.valueOf(result[7]);
		this.segmentIdentifier = new SegmentIdentifier(xway, segment, direction);
		this.position = Integer.valueOf(result[8]);
		
		this.queryIdentifier = Integer.valueOf(result[9]);
		this.sinit = Integer.valueOf(result[10]);
		this.send = Integer.valueOf(result[11]);
		this.dow = Integer.valueOf(result[12]);
		this.tod = Integer.valueOf(result[13]);
		this.day = Integer.valueOf(result[14]);
		// timer = new StopWatch(time);
		this.stormTimer = systemtimer;
		
		this.created = this.stormTimer.getElapsedTime();
	}
	
	/**
	 * Creates a tuple of the remainder of a LRB input line after the type part of the tuple string has been removed
	 * 
	 * @param type
	 * @param tupleTail
	 * @param time
	 */
	public LRBtuple(int type, String tupleTail, StopWatch time) {
		this.type = type;
		
		this.timer = new StopWatch(0);
		
		String[] result = tupleTail.split(",");
		if(result.length < 14) {
			throw new IllegalArgumentException("Tuple [" + tupleTail + "] does not match required format");
		}
		
		this.timer = time;
		
		this.vehicleIdentifier = Integer.valueOf(result[1]);
		this.currentSpeed = Integer.valueOf(result[2]);
		int xway = Integer.valueOf(result[3]);
		this.lane = Integer.valueOf(result[4]);
		int direction = Integer.valueOf(result[5]);
		int segment = Integer.valueOf(result[6]);
		this.segmentIdentifier = new SegmentIdentifier(xway, segment, direction);
		this.position = Integer.valueOf(result[7]);
		
		this.queryIdentifier = Integer.valueOf(result[8]);
		this.sinit = Integer.valueOf(result[9]);
		this.send = Integer.valueOf(result[10]);
		this.dow = Integer.valueOf(result[11]);
		this.tod = Integer.valueOf(result[12]);
		this.day = Integer.valueOf(result[13]);
	}
	
	public LRBtuple(int type, String tupleTail) {
		this(type, tupleTail, retrieveTimeFromTuple(tupleTail));
	}
	
	public Integer getType() {
		return this.type;
	}
	
	public int getTime() {
		return this.time;
	}
	
	public Integer getVehicleIdentifier() {
		return this.vehicleIdentifier;
	}
	
	public String getVidAsString() {
		return this.vehicleIdentifier.toString();
	}
	
	public int getCurrentSpeed() {
		return this.currentSpeed;
	}
	
	public Integer getLane() {
		return this.lane;
	}
	
	public Integer getPosition() {
		return this.position;
	}
	
	public Integer getQueryIdentifier() {
		return this.queryIdentifier;
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
	
	public Integer getDay() {
		return this.day;
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
	
	public StopWatch getStormTimer() {
		return this.stormTimer;
	}
	
	/**
	 * get the emit time for notification output
	 * 
	 * @return (time+processing time)
	 */
	public long getEmitTime() {
		
		return this.time + this.getProcessingTimeSec();
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
	
	public void setSegmentIdentifier(SegmentIdentifier segmentIdentifier) {
		this.segmentIdentifier = segmentIdentifier;
	}
	
	public SegmentIdentifier getSegmentIdentifier() {
		return this.segmentIdentifier;
	}
	
	@Override
	public String toString() {
		return "LRBtuple [type=" + this.type + ", created=" + this.created + ", time=" + this.time + ", vid="
			+ this.vehicleIdentifier + ", timer=" + this.timer + ", stormTimer=" + this.stormTimer + "]";
	}
	
}
