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





/**
 * Object representing account balance requests. Balance requests have the form (Type = 2, Time, VID, QID) where QID is
 * a query identifier.
 * 
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class AccountBalanceRequest extends LRBtuple {
	
	private static final long serialVersionUID = 1L;
	/**
	 * QID is an integer query identiﬁer
	 */
	private Integer queryIdentifier;
	/**
	 * VID (0. . . MAXINT) is an integer vehicle identifier i
	 */
	private Integer vehicleIdentifier;
	
	/**
	 * Time (0. . .10799)^3 is a timestamp identifying the time at which the position report was emitted
	 */
	private long time;
	
	protected AccountBalanceRequest() {
		super();
		
	}
	
	public AccountBalanceRequest(long time, int vehicleIdentifier, int queryIdentifier, StopWatch systemTimer) {
		super(LRBtuple.TYPE_ACCOUNT_BALANCE_REQUEST, System.currentTimeMillis(), systemTimer);
		this.time = time;
		this.vehicleIdentifier = vehicleIdentifier;
		this.queryIdentifier = queryIdentifier;
	}
	
	public long getTime() {
		return this.time;
	}
	
	public Integer getVehicleIdentifier() {
		return this.vehicleIdentifier;
	}
	
	public Integer getQueryIdentifier() {
		return this.queryIdentifier;
	}
	
	/**
	 * get the emit time for notification output
	 * 
	 * @return (time+processing time)
	 */
	public long getEmitTime() {
		
		return this.time + this.getProcessingTimeSec();
	}
	
}
