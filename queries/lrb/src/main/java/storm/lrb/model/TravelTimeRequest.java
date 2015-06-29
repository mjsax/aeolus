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

import storm.lrb.tools.StopWatch;
import de.hub.cs.dbis.lrb.types.AbstractInputTuple;
import de.hub.cs.dbis.lrb.types.AbstractLRBTuple;





/**
 * object to represent time travel requests
 * 
 * LRB format: (Type = 4, Time, VID, XWay, QID, S init , S end , DOW, TOD)
 */
public class TravelTimeRequest extends AbstractInputTuple {
	
	private static final long serialVersionUID = 1L;
	
	private int xWay;
	private int queryIdentifier;
	private int dayOfWeek;
	private int minuteOfDay;
	
	protected TravelTimeRequest() {
		super();
	}
	
	public TravelTimeRequest(Short time, Integer vehicleIdentifier, int xWay, int queryIdentifier, int dayOfWeek,
		int minuteOfDay, StopWatch systemTimer) {
		super(AbstractLRBTuple.TRAVEL_TIME_REQUEST, time, vehicleIdentifier);
		this.xWay = xWay;
		this.queryIdentifier = queryIdentifier;
		this.dayOfWeek = dayOfWeek;
		this.minuteOfDay = minuteOfDay;
	}
	
	public int getMinuteOfDay() {
		return this.minuteOfDay;
	}
	
	public int getDayOfWeek() {
		return this.dayOfWeek;
	}
	
	public int getQueryIdentifier() {
		return this.queryIdentifier;
	}
	
	public int getxWay() {
		return this.xWay;
	}
	
}
