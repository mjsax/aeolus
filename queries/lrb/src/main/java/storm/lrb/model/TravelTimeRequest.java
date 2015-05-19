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

import storm.lrb.tools.StopWatch;





/**
 * object to represent time travel requests
 * 
 * LRB format: (Type = 4, Time, VID, XWay, QID, S init , S end , DOW, TOD)
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class TravelTimeRequest extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private int time;
	private int vehicleIdentifier;
	private int xWay;
	private int queryIdentifier;
	private int sinit;
	private int send;
	private int dayOfWeek;
	private int minuteOfDay;
	
	protected TravelTimeRequest() {
		super();
	}
	
	public TravelTimeRequest(int time, int vehicleIdentifier, int xWay, int queryIdentifier, int sinit, int send,
		int dayOfWeek, int minuteOfDay, StopWatch systemTimer) {
		super(LRBtuple.TYPE_TRAVEL_TIME_REQUEST, System.currentTimeMillis(), systemTimer);
		this.time = time;
		this.vehicleIdentifier = vehicleIdentifier;
		this.xWay = xWay;
		this.queryIdentifier = queryIdentifier;
		this.sinit = sinit;
		this.send = send;
		this.dayOfWeek = dayOfWeek;
		this.minuteOfDay = minuteOfDay;
	}
	
	public int getMinuteOfDay() {
		return minuteOfDay;
	}
	
	public int getDayOfWeek() {
		return dayOfWeek;
	}
	
	@Override
	public Integer getSend() {
		return super.getSend(); // To change body of generated methods, choose Tools | Templates.
	}
	
	@Override
	public Integer getSinit() {
		return super.getSinit(); // To change body of generated methods, choose Tools | Templates.
	}
	
	public int getQueryIdentifier() {
		return queryIdentifier;
	}
	
	public int getxWay() {
		return xWay;
	}
	
	public int getVehicleIdentifier() {
		return vehicleIdentifier;
	}
	
	public int getTime() {
		return time;
	}
	
}
