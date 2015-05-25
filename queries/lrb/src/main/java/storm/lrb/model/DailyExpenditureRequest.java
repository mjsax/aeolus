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
 * Object to represent daily expnditure requests
 * 
 * LRB format: (Type = 3, Time, VID, XWay, QID, Day)
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class DailyExpenditureRequest extends LRBtuple implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private int vehicleIdentifier;
	private int xWay;
	private int queryIdentifier;
	/**
	 * day (1 is yesterday, 69 is 10 weeks ago)
	 */
	private Integer day;
	
	protected DailyExpenditureRequest() {
		super();
	}
	
	public DailyExpenditureRequest(long created, int vehicleIdentifier, int xWay, int queryIdentifier, int day,
		StopWatch systemTimer) {
		super(LRBtuple.TYPE_DAILY_EXPEDITURE, created, systemTimer);
		this.vehicleIdentifier = vehicleIdentifier;
		this.xWay = xWay;
		this.queryIdentifier = queryIdentifier;
		this.day = day;
	}
	
	public void setDay(int day) {
		this.day = day;
	}
	
	public int getDay() {
		return day;
	}
	
	public void setQueryIdentifier(int queryIdentifier) {
		this.queryIdentifier = queryIdentifier;
	}
	
	public int getQueryIdentifier() {
		return queryIdentifier;
	}
	
	public void setxWay(int xWay) {
		this.xWay = xWay;
	}
	
	public int getxWay() {
		return xWay;
	}
	
	public void setVehicleIdentifier(int vehicleIdentifier) {
		this.vehicleIdentifier = vehicleIdentifier;
	}
	
	public int getVehicleIdentifier() {
		return vehicleIdentifier;
	}
	
}
