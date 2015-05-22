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
/*
 * Copyright 2015 Humboldt-Universität zu Berlin.
 *
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
 */
package storm.lrb.model;

import storm.lrb.tools.StopWatch;





/**
 * The reply to a {@link AccountBalanceRequest}.
 * 
 * @author richter
 */
/*
 * internal implementation notes: - does not implement clone because Values doesn't
 */
@SuppressWarnings("CloneableImplementsClone")
public class AccountBalance extends LRBtuple {
	private static final long serialVersionUID = 1L;
	private long time;
	private StopWatch systemTimer;
	private int queryIdentifier;
	private int balance;
	private long tollTime;
	
	public AccountBalance(long time, int queryIdentifier, int balance, long tollTime, Long created,
		StopWatch systemTimer) {
		super(LRBtuple.TYPE_ACCOUNT_BALANCE, created, systemTimer);
		this.time = time;
		this.systemTimer = systemTimer;
		this.queryIdentifier = queryIdentifier;
		this.balance = balance;
		this.tollTime = tollTime;
	}
	
	public void setTollTime(long tollTime) {
		this.tollTime = tollTime;
	}
	
	public long getTollTime() {
		return tollTime;
	}
	
	protected void setTime(long time) {
		this.time = time;
	}
	
	protected void setSystemTimer(StopWatch systemTimer) {
		this.systemTimer = systemTimer;
	}
	
	protected void setQueryIdentifier(int queryIdentifier) {
		this.queryIdentifier = queryIdentifier;
	}
	
	protected void setBalance(int balance) {
		this.balance = balance;
	}
	
	public long getTime() {
		return time;
	}
	
	public StopWatch getSystemTimer() {
		return systemTimer;
	}
	
	public int getQueryIdentifier() {
		return queryIdentifier;
	}
	
	public int getBalance() {
		return balance;
	}
	
	
}
