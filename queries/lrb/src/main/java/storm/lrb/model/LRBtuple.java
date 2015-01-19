package storm.lrb.model;

/*
 * #%L
 * lrb
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

import java.io.Serializable;
import java.util.Arrays;

import storm.lrb.tools.StopWatch;

/**
 * superclass for all requests 
 *
 */
public class LRBtuple  implements Serializable{

	
	private static final long serialVersionUID = 1L;

	/**
	 * tuple type 0=Position report 2=Account balance requests 3=daily
	 * expenditure request 4=Travel time request
	 */
	private Integer type;
	
	/**
	 * time of creation (in the storm application)
	 */
	private Long created;

	/**
	 * Time (0. . .10799)^3 is a timestamp identifying the time at which the
	 * position report was emitted
	 */
	private int time;

	/**
	 * VID (0. . . MAXINT) is an integer vehicle identifier i
	 */
	private Integer vid;

	/**
	 * Spd (0. . .100) is an integer reﬂecting the speed of the vehicle (in MPH)
	 * at the time the position report
	 */
	private int spd;
	/**
	 * XWay (0. . . L−1) identifies the expressway from which the position
	 * report is emitted
	 */
	private Integer xway;
	/**
	 * Lane (0. . .4) identiﬁes the lane of the expressway from which the
	 * position report is emitted 0 if it is an entrance ramp (ENTRY), 1 − 3 if
	 * it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT).
	 */
	private Integer lane;
	/**
	 * Dir (0. . .1) indicates the direction (0 for Eastbound and 1 for
	 * Westbound)
	 */
	private Integer dir;
	/**
	 * Seg (0. . .99) identiﬁes the mile-long segment from which the position
	 * report is emitted
	 */
	private Integer seg;
	/**
	 * Pos (0. . .527999) identiﬁes the horizontal position of the vehicle as a
	 * measure of the number of feet from the western most point on the
	 * expressway (i.e.,Pos = x)
	 */
	private Integer pos;
	/**
	 * QID is an integer query identiﬁer
	 */
	private Integer qid;
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
	 * TOD (1. . .1440) specifies the day of the week and minute number in the
	 * day when the journey would take place
	 */
	private Integer tod;
	/**
	 * day (1 is yesterday, 69 is 10 weeks ago)
	 */
	private Integer day;

	StopWatch timer;
	
	StopWatch stormTimer = null;

	public LRBtuple(String tupel) {

		timer = new StopWatch(0);

		String[] result = tupel.split(",");
		if (result.length < 15) {
			throw new IllegalArgumentException(
					"Tuple ["+tupel+"] does not match required format");
		}

		type = Integer.valueOf(result[0]);
		time = Integer.valueOf(result[1]);
		timer.setOffset(time);
		
		vid = Integer.valueOf(result[2]);
		spd = Integer.valueOf(result[3]);
		xway = Integer.valueOf(result[4]);
		lane = Integer.valueOf(result[5]);
		dir = Integer.valueOf(result[6]);
		seg = Integer.valueOf(result[7]);
		pos = Integer.valueOf(result[8]);

		qid = Integer.valueOf(result[9]);
		sinit = Integer.valueOf(result[10]);
		send = Integer.valueOf(result[11]);
		dow = Integer.valueOf(result[12]);
		tod = Integer.valueOf(result[13]);
		day = Integer.valueOf(result[14]);
		//timer = new StopWatch(time);
		
		

	}

	public LRBtuple(){
		//kryo needs empty constructor
	}
	
	public LRBtuple(String tupel, StopWatch systemtimer) {
		timer = new StopWatch(0);

		String[] result = tupel.split(",");
		//System.out.println("splitted "+Arrays.toString(result));
		if (result.length < 15) {
			throw new IllegalArgumentException(
					"Tuple does not match required format");
		}

		type = Integer.valueOf(result[0]);
		time = Integer.valueOf(result[1]);
		timer.setOffset(time);

		vid = Integer.valueOf(result[2]);
		spd = Integer.valueOf(result[3]);
		xway = Integer.valueOf(result[4]);
		lane = Integer.valueOf(result[5]);
		dir = Integer.valueOf(result[6]);
		seg = Integer.valueOf(result[7]);
		pos = Integer.valueOf(result[8]);

		qid = Integer.valueOf(result[9]);
		sinit = Integer.valueOf(result[10]);
		send = Integer.valueOf(result[11]);
		dow = Integer.valueOf(result[12]);
		tod = Integer.valueOf(result[13]);
		day = Integer.valueOf(result[14]);
		//timer = new StopWatch(time);
		stormTimer = systemtimer;
		
		created = stormTimer.getElapsedTime();
	}

	public Integer getType() {
		return type;
	}

	public int getTime() {
		return time;
	}

	public Integer getVid() {
		return vid;
	}
	
	public String getVidAsString() {
		return vid.toString();
	}

	public int getSpd() {
		return spd;
	}

	public Integer getXway() {
		return xway;
	}

	public Integer getLane() {
		return lane;
	}

	public Integer getDir() {
		return dir;
	}

	public Integer getSeg() {
		return seg;
	}

	public Integer getPos() {
		return pos;
	}

	public Integer getQid() {
		return qid;
	}

	public Integer getSinit() {
		return sinit;
	}

	public Integer getSend() {
		return send;
	}

	public Integer getDow() {
		return dow;
	}

	public Integer getTod() {
		return tod;
	}

	public Integer getDay() {
		return day;
	}

	public StopWatch getTimer() {
		return timer;
	}
	
	/**
	 * Time of creation. (actual running time of the simulation)
	 * @return
	 */
	public Long getCreated() {
		return created;
	}
	public StopWatch getStormTimer() {
		return stormTimer;
	}
	
	/**
	 * get the emit time for notification output
	 * @return (time+processing time)
	 */
	public long getEmitTime() {

		return time + this.getProcessingTimeSec();
	}

	/**
	 * get the time it took to process this tuple in ms
	 * @return processing time in ms
	 */
	public long getProcessingTime() {
		return timer.getDurationTime();
	}
	
	/**
	 * get the time it took to process this tuple in ms
	 * @return processing time in seconds.
	 */
	public long getProcessingTimeSec() {
		return timer.getDurationTimeSecs();
	}

	@Override
	public String toString() {
		return "LRBtuple [type=" + type + ", created=" + created + ", time="
				+ time + ", vid=" + vid + ", timer=" + timer + ", stormTimer="
				+ stormTimer + "]";
	}

	
}
