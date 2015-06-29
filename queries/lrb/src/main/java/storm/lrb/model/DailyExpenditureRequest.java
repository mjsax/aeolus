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

import de.hub.cs.dbis.lrb.types.AbstractInputTuple;
import de.hub.cs.dbis.lrb.types.AbstractLRBTuple;





/**
 * Object to represent daily expnditure requests
 * 
 * LRB format: (Type = 3, Time, VID, XWay, QID, Day)
 */
public class DailyExpenditureRequest extends AbstractInputTuple {
	
	private static final long serialVersionUID = 1L;
	
	private int xWay;
	private int queryIdentifier;
	/**
	 * day (1 is yesterday, 69 is 10 weeks ago)
	 */
	private Integer day;
	
	protected DailyExpenditureRequest() {
		super();
	}
	
	public DailyExpenditureRequest(Short time, Integer vehicleIdentifier, int xWay, int queryIdentifier, int day) {
		super(AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, time, vehicleIdentifier);
		this.xWay = xWay;
		this.queryIdentifier = queryIdentifier;
		this.day = day;
	}
	
	public void setDay(int day) {
		this.day = day;
	}
	
	public int getDay() {
		return this.day;
	}
	
	public void setQueryIdentifier(int queryIdentifier) {
		this.queryIdentifier = queryIdentifier;
	}
	
	public int getQueryIdentifier() {
		return this.queryIdentifier;
	}
	
	public void setxWay(int xWay) {
		this.xWay = xWay;
	}
	
	public int getxWay() {
		return this.xWay;
	}
	
}
